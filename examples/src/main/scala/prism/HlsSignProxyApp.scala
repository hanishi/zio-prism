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
 * Composing the two rewrite styles: `HlsProxyApp` re-points every URL in a live manifest back
 * through the proxy (value-dependent string work — resolve a relative ref, route by extension).
 * This rung takes that *already-proxied* manifest and adds the other half a real edge does —
 * '''URL signing''' — as a streaming literal pass.
 *
 * Why two different tools. Routing can't be a literal swap: a relative `key1.json` has no host to
 * anchor on, so the proxy must *resolve* it first. Signing is the opposite — every outbound media
 * URL already shares a known prefix (`/seg?u=…`), so appending an expiring token to each is a pure
 * literal transform over a fixed anchor. That is exactly `Rewrite.wrappingUrls`, and run through
 * `RewriteStream.pipeline` it stays correct even when the playlist is streamed in tiny chunks that
 * split a URL across chunk boundaries — the same chunk-boundary proof `HlsManifestApp` makes.
 *
 * So a production edge is: '''resolve + route''' (HlsProxyApp) then '''sign''' (here), the signer
 * seeing the already-routed URLs. The token ends a URL at whitespace (a bare segment line) and at a
 * quote (a `URI="…"` value), so one rewriter signs both the segment lines and the tag-embedded URLs
 * (`#EXT-X-MAP`, `#EXT-X-KEY`). The proxied URL already carries `?u=…`, so `&` is the right
 * separator for the token.
 *
 * Run with: `sbt "examples/runMain prism.HlsSignProxyApp"`
 */
object HlsSignProxyApp extends ZIOAppDefault {

  // A media playlist as HlsProxyApp emits it: every media URL already re-pointed to `/seg?u=<enc>`,
  // including the init segment (#EXT-X-MAP) and the decryption key (#EXT-X-KEY).
  private val proxied =
    """#EXTM3U
      |#EXT-X-VERSION:7
      |#EXT-X-TARGETDURATION:10
      |#EXT-X-MEDIA-SEQUENCE:42
      |#EXT-X-MAP:URI="/seg?u=https%3A%2F%2Fcdn.vendor.com%2F720p%2Finit.mp4"
      |#EXT-X-KEY:METHOD=AES-128,URI="/seg?u=https%3A%2F%2Fcdn.vendor.com%2Fkeys%2F42.bin",IV=0x9c7d2f1a4b6e8051
      |#EXTINF:10.000,
      |/seg?u=https%3A%2F%2Fcdn.vendor.com%2F720p%2Fseg00042.m4s
      |#EXTINF:10.000,
      |/seg?u=https%3A%2F%2Fcdn.vendor.com%2F720p%2Fseg00043.m4s
      |#EXTINF:10.000,
      |/seg?u=https%3A%2F%2Fcdn.vendor.com%2F720p%2Fseg00044.m4s""".stripMargin

  // Sign every proxied media URL (those on the `/seg?u=` anchor) with an expiring token. One
  // rewriter covers both the bare segment lines and the quoted URIs inside tags.
  private val signer =
    Rewrite.wrappingUrls("/seg?u=", "{url}&token=a1b2c3d4&exp=1718600000")

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
      out <- chunked(proxied, n = 7)
               .via(RewriteStream.pipeline(signer))
               .runCollect
               .map(c => new String(c.toArray, UTF_8))
      signed = countOccurrences(out, "&token=a1b2c3d4")
      _ <- Console.printLine("--- proxied (input: every URL already routed to /seg) ---")
      _ <- Console.printLine(proxied)
      _ <- Console.printLine("")
      _ <- Console.printLine("--- signed (streamed in 7-byte chunks) ---")
      _ <- Console.printLine(out)
      _ <- Console.printLine("")
      _ <- Console.printLine(s"signed $signed media URL(s) with an expiring token (segments, init, and key)")
    } yield ()
}
