/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package prism

import zio.*

/**
 * Server-side ad insertion (SSAI), the manifest half: at an ad cue in a live media playlist,
 * splice a pre-conditioned ad pod into the stream — bracketed by `#EXT-X-DISCONTINUITY` so the
 * player treats it as one continuous stream of segments and can't tell content from ad.
 *
 * The architecture is the "precompute, then serve from a warm index" pattern, not a live auction:
 *
 *   - '''Ahead of time''' the expensive work — transcoding each advertiser creative into
 *     format-matched, fragmented segments — has already run, and the results sit in `availIndex`,
 *     keyed by *content context* (category / daypart), which is known before any viewer arrives.
 *     The index only ever holds creatives that are already conditioned and ready to splice.
 *   - '''At splice time''' the work is cheap: look the avail up by context, *select* a pod (here,
 *     highest expected value — a deterministic stand-in for the Thompson sampling a real serve path
 *     would use to keep learning), *greedily fill* segments up to the cue's duration, and emit the
 *     discontinuity-bracketed splice. No transcoding on the hot path.
 *   - '''Cold miss''' — a context with nothing conditioned yet — falls back to a pre-conditioned
 *     '''slate''' (house filler) instead of stalling. Because conditioning is a precondition for
 *     entering the index, "the creative isn't ready" is the *only* miss, and the slate covers it;
 *     a real server would also kick off conditioning so the next viewer is a hit.
 *
 * Two scenarios are run: a warm context that splices a real pod, and a cold context that falls back
 * to slate — the whole hit/miss behavior of the pattern in one output.
 *
 * Returning to content after the break re-declares the content init segment (`#EXT-X-MAP`), because
 * the ad pod carried its own init across the discontinuity.
 *
 * Run with: `sbt "examples/runMain prism.HlsAdStitchApp"`
 */
object HlsAdStitchApp extends ZIOAppDefault {

  private final case class AdSeg(uri: String, dur: Double)
  private final case class AdPod(id: String, score: Double, init: String, segs: List[AdSeg])

  private def pod(id: String, score: Double, n: Int): AdPod =
    AdPod(
      id,
      score,
      s"https://ads.cdn.example/$id/init.mp4",
      (0 until n).map(i => AdSeg(s"https://ads.cdn.example/$id/$i.m4s", 4.0)).toList
    )

  // The warm index: pre-conditioned ad pods keyed by content context. This is the whole point —
  // the costly transcode/auction ran ahead of time; serve-time only reads this map.
  private val availIndex: Map[String, List[AdPod]] =
    Map(
      "sports-primetime" -> List(
        pod("nike-air", score = 0.82, n = 4),  // 4x4s = 16s of conditioned ad available
        pod("toyota-hybrid", score = 0.74, n = 4)
      )
    )

  // Pre-conditioned house filler for cold misses — always ready, never stalls the break.
  private val slate = pod("house-slate", score = 0.0, n = 3)

  /** Serve-time selection: highest expected value wins (a stand-in for Thompson sampling). */
  private def select(context: String): (AdPod, Boolean) =
    availIndex.get(context).flatMap(_.maxByOption(_.score)) match {
      case Some(p) => (p, false) // warm hit
      case None    => (slate, true) // cold miss -> slate fallback
    }

  /** Greedily take whole segments up to the avail duration. */
  private def fill(p: AdPod, availDur: Double): List[AdSeg] = {
    val (taken, _) = p.segs.foldLeft((List.empty[AdSeg], 0.0)) { case ((acc, used), s) =>
      if (used + s.dur <= availDur + 1e-6) (acc :+ s, used + s.dur) else (acc, used)
    }
    taken
  }

  private val CueOut = """#EXT-X-CUE-OUT:([0-9.]+)""".r

  private final case class Report(context: String, podId: String, slate: Boolean, segs: Int, dur: Double)

  /** Walk the playlist; at each `#EXT-X-CUE-OUT:<dur>` splice the selected pod for `context`. */
  private def stitch(playlist: String, context: String): (String, List[Report]) = {
    val contentInit = playlist.linesIterator.find(_.startsWith("#EXT-X-MAP:")).getOrElse("")
    val reports     = scala.collection.mutable.ListBuffer.empty[Report]
    val out = playlist.linesIterator.flatMap { line =>
      line.trim match {
        case CueOut(d) =>
          val availDur     = d.toDouble
          val (chosen, sl) = select(context)
          val segs         = fill(chosen, availDur)
          reports += Report(context, chosen.id, sl, segs.size, segs.map(_.dur).sum)
          // The discontinuity-bracketed splice: into the ad (its own init), then back to content.
          val adBlock =
            List(line, "#EXT-X-DISCONTINUITY", s"""#EXT-X-MAP:URI="${chosen.init}"""") ++
              segs.flatMap(s => List(f"#EXTINF:${s.dur}%.3f,", s.uri)) ++
              List("#EXT-X-DISCONTINUITY", contentInit)
          adBlock
        case _ => List(line)
      }
    }.mkString("\n")
    (out, reports.toList)
  }

  // A live content media playlist with one ad avail marked by a CUE-OUT/CUE-IN pair.
  private val content =
    """#EXTM3U
      |#EXT-X-VERSION:7
      |#EXT-X-TARGETDURATION:6
      |#EXT-X-MEDIA-SEQUENCE:100
      |#EXT-X-MAP:URI="https://video.publisher.com/content/init.mp4"
      |#EXTINF:6.000,
      |https://video.publisher.com/content/seg100.m4s
      |#EXTINF:6.000,
      |https://video.publisher.com/content/seg101.m4s
      |#EXT-X-CUE-OUT:12.000
      |#EXT-X-CUE-IN
      |#EXTINF:6.000,
      |https://video.publisher.com/content/seg102.m4s
      |#EXTINF:6.000,
      |https://video.publisher.com/content/seg103.m4s""".stripMargin

  private def runScenario(label: String, context: String): UIO[Unit] = {
    val (out, reports) = stitch(content, context)
    val r              = reports.head
    val verdict        =
      if (r.slate) s"COLD MISS -> slate '${r.podId}'"
      else s"warm hit -> pod '${r.podId}'"
    Console.printLine(s"=== $label  (context=\"$context\") ===").orDie *>
      Console.printLine(s"avail ${"%.1f".format(12.0)}s  $verdict  filled ${r.segs} seg(s) = ${"%.1f".format(r.dur)}s").orDie *>
      Console.printLine("").orDie *>
      Console.printLine(out).orDie *>
      Console.printLine("").orDie
  }

  def run =
    runScenario("WARM: context is in the index", "sports-primetime") *>
      runScenario("COLD: context not conditioned yet", "late-night-niche")
}
