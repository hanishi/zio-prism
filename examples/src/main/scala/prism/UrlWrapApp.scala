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
import zio.stream.ZStream

import java.nio.charset.StandardCharsets.UTF_8

/**
 * Capture-and-transform: wrap measurement URLs behind a first-party collector as they stream.
 *
 * `Rewrite.wrappingUrls(anchor, template)` finds every URL that starts with `anchor`, captures it
 * whole (up to the next URL boundary — a quote, `<`, `]`, whitespace…), and rewrites it through
 * `template`, where `{enc}` is the original URL-encoded into the new URL's query string. This is
 * how a proxy re-points third-party ad/measurement beacons at a first-party endpoint that then
 * forwards the hit. URLs on other hosts are left byte-for-byte untouched.
 *
 * The motivating format is VAST (ad XML), where tracker URLs sit inside `<![CDATA[…]]>`. To prove
 * the streaming guarantee, the document is fed through the pipeline in deliberately tiny chunks,
 * so most tracker URLs are split across chunk boundaries — yet each is still captured and
 * transformed whole, because the engine holds an unterminated token as carry until its boundary
 * arrives. The output is identical to a one-shot rewrite, at any chunk size.
 *
 * Run with: `sbt "examples/runMain prism.UrlWrapApp"`
 */
object UrlWrapApp extends ZIOAppDefault {

  private val rewriter = Rewrite.wrappingUrls(
    "https://tracker.example.com",
    "https://fp.publisher.com/collect?dest={enc}"
  )

  // Three tracker URLs to wrap (in CDATA and element text) plus one media URL on another host
  // that must pass through unchanged.
  private val doc =
    """<VAST version="4.0">
      |  <Ad><InLine>
      |    <Impression><![CDATA[https://tracker.example.com/imp?cb=12345&pub=acme]]></Impression>
      |    <Tracking event="start"><![CDATA[https://tracker.example.com/ev/start]]></Tracking>
      |    <MediaFiles>
      |      <MediaFile><![CDATA[https://cdn.vendor.com/creative/ad-720p.mp4]]></MediaFile>
      |    </MediaFiles>
      |    <ClickThrough>https://tracker.example.com/click?to=landing</ClickThrough>
      |  </InLine></Ad>
      |</VAST>""".stripMargin

  /** Stream `s` one tiny `n`-byte chunk at a time, splitting mid-URL on purpose. */
  private def chunked(s: String, n: Int): ZStream[Any, Nothing, Byte] = {
    val bytes = Chunk.fromArray(s.getBytes(UTF_8))
    ZStream.fromChunks((bytes.indices by n).map(i => bytes.slice(i, math.min(i + n, bytes.length)))*)
  }

  private def countOccurrences(haystack: String, needle: String): Int =
    if (needle.isEmpty) 0
    else {
      var count = 0
      var idx   = haystack.indexOf(needle)
      while (idx >= 0) { count += 1; idx = haystack.indexOf(needle, idx + needle.length) }
      count
    }

  def run =
    for {
      // 7-byte chunks: small enough that every tracker URL straddles several chunk boundaries.
      out <- chunked(doc, n = 7)
               .via(RewriteStream.pipeline(rewriter))
               .runCollect
               .map(c => new String(c.toArray, UTF_8))
      wrapped = countOccurrences(out, "https://fp.publisher.com/collect?dest=")
      _ <- Console.printLine("--- input ---")
      _ <- Console.printLine(doc)
      _ <- Console.printLine("")
      _ <- Console.printLine("--- rewritten (streamed in 7-byte chunks) ---")
      _ <- Console.printLine(out)
      _ <- Console.printLine("")
      _ <- Console.printLine(s"wrapped $wrapped tracker URL(s) into the first-party collector")
      _ <- Console.printLine("left the cdn.vendor.com media URL untouched")
    } yield ()
}
