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
 * HLS manifest rewriting: the two operations a video proxy actually performs on an `.m3u8`, run in
 * flight as the playlist streams.
 *
 *   1. '''Re-point the CDN host''' — `Rewrite.literal` swaps `cdn.vendor.com` for a first-party
 *      `video.publisher.com`. A manifest is all directives and URLs with no prose, so the host
 *      string appears *only* inside URLs; a plain literal swap is correct here with no
 *      attribute-scoping (unlike HTML, where `Rewrite.replacingHost` is needed to avoid rewriting
 *      body text — see `HostRewriteApp`).
 *   2. '''Sign every URL''' — `Rewrite.wrappingUrls` captures each URL on the re-pointed host and
 *      appends an expiring token via `{url}`. Its URL boundary ends a token at whitespace (a bare
 *      `#EXTINF` segment line) *and* at a quote (a `URI="…"` value), so one rewriter signs both the
 *      media segments and the tag-embedded URLs — the init segment (`#EXT-X-MAP`) and the
 *      decryption key (`#EXT-X-KEY`) — and the token lands inside the quotes.
 *
 * The two passes are stacked as independent pipelines, each with its own carry, so the token
 * anchor sees the already-re-pointed host.
 *
 * Streaming is the point for *live* HLS: a live media playlist is re-fetched continuously and each
 * refresh must be rewritten before it reaches the player, with the large opaque segment bodies
 * never touched. To prove boundary correctness the playlist is fed in 7-byte chunks, so every URL
 * straddles several chunk boundaries yet each is re-pointed and signed whole.
 *
 * Honest scope: appending `?token=…` assumes the URL carries no query of its own (true for the
 * path-style URLs below); a real signer would append with `&` when a query is already present.
 *
 * Run with: `sbt "examples/runMain prism.HlsManifestApp"`
 */
object HlsManifestApp extends ZIOAppDefault {

  // 1. Re-point the third-party CDN at a first-party host.
  private val hostSwap =
    Rewrite.literal(Seq("https://cdn.vendor.com" -> "https://video.publisher.com"))

  // 2. Sign every URL on the re-pointed host with an expiring token (path-style URLs, no existing
  //    query — so `?token=` is the correct separator).
  private val signUrls =
    Rewrite.wrappingUrls("https://video.publisher.com", "{url}?token=a1b2c3d4&exp=1718600000")

  // A live-style media playlist: an init segment and a decryption key (URLs inside tag attributes)
  // plus three media segments (bare-line URLs). No #EXT-X-ENDLIST — it is still live.
  private val playlist =
    """#EXTM3U
      |#EXT-X-VERSION:7
      |#EXT-X-TARGETDURATION:6
      |#EXT-X-MEDIA-SEQUENCE:42
      |#EXT-X-MAP:URI="https://cdn.vendor.com/720p/init.mp4"
      |#EXT-X-KEY:METHOD=AES-128,URI="https://cdn.vendor.com/keys/42.bin",IV=0x9c7d2f1a4b6e8051
      |#EXTINF:6.000,
      |https://cdn.vendor.com/720p/seg00042.m4s
      |#EXTINF:6.000,
      |https://cdn.vendor.com/720p/seg00043.m4s
      |#EXTINF:6.000,
      |https://cdn.vendor.com/720p/seg00044.m4s""".stripMargin

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
      // 7-byte chunks: small enough that every URL straddles several chunk boundaries. Stack the
      // host re-point and the signer as two independent pipelines.
      out <- chunked(playlist, n = 7)
               .via(RewriteStream.pipeline(hostSwap))
               .via(RewriteStream.pipeline(signUrls))
               .runCollect
               .map(c => new String(c.toArray, UTF_8))
      repointed = countOccurrences(out, "https://video.publisher.com")
      signed    = countOccurrences(out, "?token=a1b2c3d4")
      leaked    = countOccurrences(out, "cdn.vendor.com")
      _ <- Console.printLine("--- input ---")
      _ <- Console.printLine(playlist)
      _ <- Console.printLine("")
      _ <- Console.printLine("--- rewritten (streamed in 7-byte chunks) ---")
      _ <- Console.printLine(out)
      _ <- Console.printLine("")
      _ <- Console.printLine(s"re-pointed $repointed URL(s) to the first-party host, signed $signed")
      _ <- Console.printLine(s"left $leaked reference(s) to the original CDN host")
    } yield ()
}
