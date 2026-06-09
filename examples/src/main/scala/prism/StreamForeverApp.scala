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
import zio.stream.{ZSink, ZStream}

import java.io.{IOException, InputStream}
import java.nio.file.Paths
import java.net.URI
import java.net.http.{HttpClient, HttpRequest, HttpResponse}
import java.nio.charset.StandardCharsets.UTF_8

/**
 * Endless real-stream sample: rewrite a live, unbounded feed forever, in constant memory.
 *
 * The source is Wikimedia EventStreams, a public, no-auth Server-Sent Events feed of every
 * edit across all Wikimedia wikis. It never ends: the ideal "very large document" that is
 * genuinely unbounded. Every event carries wiki hostnames (`meta.uri`), so we rewrite those
 * as they stream past, exactly the way a reverse proxy re-points an origin's links.
 *
 * It runs until you Ctrl-C, printing bytes rewritten and JVM heap roughly every two seconds,
 * plus a live before -> after example pulled from each window so you can watch the rewrite
 * happen. The heap stays flat no matter how long it runs: the carry never exceeds the longest
 * pattern, so an infinite stream is rewritten in a few bytes of state.
 *
 * Run with: `sbt "runMain prism.StreamForeverApp"`
 */
object StreamForeverApp extends ZIOAppDefault {

  private val url = "https://stream.wikimedia.org/v2/stream/recentchange"

  // Independent, multi-byte patterns (none a substring of another) -> Wu-Manber dispatch.
  // These hostnames appear in nearly every event, so the rewriter does real work each chunk.
  private val rules = Seq(
    "wikipedia.org"  -> "ziopedia.org",
    "wikimedia.org"  -> "ziomedia.org",
    "wikinews.org"   -> "zionews.org",
    "wiktionary.org" -> "zionary.org"
  )
  private val rewriter = Rewrite.literal(rules)

  private def fetch(u: String): ZIO[Any, IOException, InputStream] =
    ZIO.attemptBlockingIO {
      // HTTP/1.1 (not the JDK default HTTP/2, on which this SSE stream stalls after the first
      // event) and a descriptive User-Agent, which Wikimedia asks API clients to send.
      val client = HttpClient.newBuilder
        .version(HttpClient.Version.HTTP_1_1)
        .followRedirects(HttpClient.Redirect.NORMAL)
        .build
      val req = HttpRequest
        .newBuilder(URI.create(u))
        .header("User-Agent", "zio-prism-sample/0.1 (https://github.com/hanishi/zio-prism)")
        .header("Accept", "text/event-stream")
        .GET
        .build
      client.send(req, HttpResponse.BodyHandlers.ofInputStream).body
    }

  /** Like `ZStream.fromInputStreamZIO`, but the blocking `read` is *cancelable*: on interruption
    * (Ctrl-C) the cancel action closes the stream, which makes the socket `read` throw and unblocks
    * the thread immediately. The built-in source uses non-cancelable blocking IO, so a fiber parked
    * waiting for the next SSE event ignores interruption and the app hangs. */
  private def interruptibleBytes(
    acquire: ZIO[Any, IOException, InputStream],
    chunkSize: Int
  ): ZStream[Any, IOException, Byte] =
    ZStream.unwrapScoped {
      ZIO.acquireRelease(acquire)(is => ZIO.succeed(is.close()).ignore).map { is =>
        ZStream.repeatZIOChunkOption {
          val buf = new Array[Byte](chunkSize)
          ZIO
            .attemptBlockingCancelable(is.read(buf))(ZIO.succeed(is.close()).ignore)
            .refineToOrDie[IOException]
            .asSomeError
            .flatMap {
              case -1 => ZIO.fail(None) // EOF -> end the stream
              case n  => ZIO.succeed(Chunk.fromArray(java.util.Arrays.copyOf(buf, n)))
            }
        }
      }
    }

  private def mb(bytes: Long): Double = bytes.toDouble / (1024 * 1024)

  // Map each rewritten host back to its original, to render "before -> after" for display.
  private val reverse = rules.map { case (from, to) => to -> from }

  private def isHostChar(c: Char): Boolean = c.isLetterOrDigit || c == '.' || c == '-'

  /** Pull one rewritten host out of a window of output bytes and show it next to its original,
    * e.g. `en.wikipedia.org  ->  en.wikipedia.mirror`. */
  private def sampleRewrite(window: Chunk[Byte]): Option[String] = {
    val text = new String(window.toArray, UTF_8)
    val idx  = text.indexOf(".org")
    if (idx < 0) None
    else {
      var s = idx
      while (s > 0 && isHostChar(text.charAt(s - 1))) s -= 1
      var e = idx + ".org".length
      while (e < text.length && isHostChar(text.charAt(e))) e += 1
      val after  = text.substring(s, e)
      val before = reverse.foldLeft(after) { case (h, (to, from)) => h.replace(to, from) }
      Some(s"$before  ->  $after")
    }
  }

  // Retained (live) heap: nudge a GC first so we measure what is *held*, not transient
  // per-chunk garbage. This is the number that proves bounded memory: it stays flat no
  // matter how many MB have streamed, because the only retained state is the carry.
  private def liveHeapMb: Double = {
    val rt = java.lang.Runtime.getRuntime
    rt.gc()
    mb(rt.totalMemory - rt.freeMemory)
  }

  // Default output file when no path is given on the command line.
  private val defaultOut = "rewritten.out"

  def run: ZIO[ZIOAppArgs, IOException, Unit] =
    for {
      args <- getArgs
      // First positional arg is the output path; falls back to `rewritten.out`.
      outPath = Paths.get(args.headOption.getOrElse(defaultOut))
      _     <- Console.printLine(s"streaming (and rewriting) $url forever  (Ctrl-C to stop)")
      _     <- Console.printLine(s"writing rewritten stream to ${outPath.toAbsolutePath}")
      total <- Ref.make(0L)
      // Tee the rewritten bytes into a file sink as they pass, then keep the live progress
      // readout on stdout. The file grows until interruption; heap stays flat (only the carry
      // is retained), so this runs in constant memory no matter how long you leave it.
      _ <- interruptibleBytes(fetch(url), chunkSize = 16 * 1024)
             .via(RewriteStream.pipeline(rewriter))
             .tapSink(ZSink.fromPath(outPath))
             .groupedWithin(chunkSize = Int.MaxValue, within = 2.seconds)
             .runForeach { window =>
               for {
                 n <- total.updateAndGet(_ + window.size)
                 _ <- Console.printLine(f"${mb(n)}%,.2f MB rewritten   live heap ${liveHeapMb}%,.0f MB")
                 _ <- ZIO.foreachDiscard(sampleRewrite(window))(s => Console.printLine(s"    e.g.  $s"))
               } yield ()
             }
             .refineToOrDie[IOException]
    } yield ()
}