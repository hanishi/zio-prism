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

import zio.Chunk

import java.nio.charset.{Charset, StandardCharsets}
import java.util.Arrays

/**
 * Streaming '''capture-and-transform''' rewriter: find a token that starts at one of `anchors`,
 * capture it up to the next boundary byte, and replace it with `transform(captured)`. Unlike
 * [[LiteralRewriter]] (static `from -> to`), the replacement is a function of what was actually
 * there — which is what "capture the original" needs.
 *
 * The motivating case is first-party proxying of ad/measurement URLs: anchor on
 * `https://tracker.example.com`, capture the whole URL, and emit
 * `https://fp.publisher.com/collect?dest=<original-url-encoded>` so a first-party endpoint can
 * forward the hit. See [[TokenRewriter.wrappingUrls]].
 *
 * Streaming contract: when the token has no boundary yet in the buffer and we are not at EOF and
 * still under [[maxTokenLength]], the anchor and everything after it are held as carry (the "need
 * more" case). Carry is bounded by `maxTokenLength`. On EOF an unterminated token is captured
 * as-is; over budget it is emitted verbatim rather than transformed. Output is assembled into a
 * raw byte buffer via `System.arraycopy`; the unmatched path returns a zero-copy `input.take`
 * slice.
 *
 * @param anchors        token start markers (e.g. `Seq("http://","https://")`)
 * @param transform      captured token in, replacement out
 * @param isBoundary     byte that ends a token (default: URL delimiters)
 * @param maxTokenLength max bytes captured after an anchor before giving up
 */
final class TokenRewriter(
    anchors: Seq[String],
    transform: String => String,
    isBoundary: Byte => Boolean = TokenRewriter.UrlBoundary,
    charset: Charset = StandardCharsets.UTF_8,
    maxTokenLength: Int = 8192
) extends Rewriter {

  require(anchors.nonEmpty, "at least one anchor required")

  private val ac = AhoCorasick(anchors.map(_.getBytes(charset)))

  def apply(input: Chunk[Byte], atEOF: Boolean): (Chunk[Byte], Int) = {
    if (input.isEmpty) return (Chunk.empty, 0)

    val bytes = input.toArray
    val len   = bytes.length

    var out    = new Array[Byte](len)
    var outPos = 0
    def append(src: Array[Byte], from: Int, count: Int): Unit = {
      if (outPos + count > out.length) {
        var n = math.max(out.length * 2, outPos + count)
        while (n < outPos + count) n *= 2
        out = Arrays.copyOf(out, n)
      }
      System.arraycopy(src, from, out, outPos, count)
      outPos += count
    }

    var state    = ac.root
    var lastEmit = 0

    // Build the (output, consumed) result for a return at `end`. With nothing replaced yet
    // (lastEmit still 0) the output is the unchanged prefix — return a zero-copy slice.
    def finish(end: Int): (Chunk[Byte], Int) =
      if (lastEmit == 0) (input.take(end), end)
      else {
        if (lastEmit < end) append(bytes, lastEmit, end - lastEmit)
        (Chunk.fromArray(Arrays.copyOf(out, outPos)), end)
      }

    // Emit [lastEmit, start) verbatim, then the transformed token for [start, end); advance.
    def replace(start: Int, end: Int): Unit = {
      if (start > lastEmit) append(bytes, lastEmit, start - lastEmit)
      val token = new String(bytes, start, end - start, charset)
      val repl  = transform(token).getBytes(charset)
      append(repl, 0, repl.length)
      lastEmit = end
    }

    var i = 0
    while (i < len) {
      state = ac.step(state, bytes(i))
      val mlen = ac.matchLenAt(state)
      if (mlen > 0) {
        val anchorStart = i - mlen + 1
        if (anchorStart >= lastEmit) {
          // capture from the anchor start until a boundary / budget / end of input
          var k   = i + 1
          var end = -1 // -1: need more, -2: over budget, >=0: boundary index
          while (k < len && end == -1) {
            if (isBoundary(bytes(k))) end = k
            else if (k - anchorStart >= maxTokenLength) end = -2
            else k += 1
          }
          if (end == -2) {
            state = ac.root // token too long → leave verbatim, keep scanning
          } else if (end >= 0) {
            replace(anchorStart, end)
            i = end - 1
            state = ac.root
          } else if (atEOF) {
            replace(anchorStart, len) // token runs to EOF → capture it whole
            i = len - 1
            state = ac.root
          } else {
            return finish(anchorStart) // boundary not here yet → hold for next chunk
          }
        }
      }
      i += 1
    }

    if (atEOF) finish(len)
    else {
      // hold back a possible partial anchor at the tail
      val safe = math.max(lastEmit, len - ac.depthAt(state))
      finish(safe)
    }
  }
}

object TokenRewriter {

  /** A URL ends at whitespace/control, quotes, `<`, `>`, `]` (CDATA close) or a backtick. */
  val UrlBoundary: Byte => Boolean = b => {
    val u = b & 0xff
    u <= 0x20 || u == '"' || u == '\'' || u == '<' || u == '>' || u == ']' || u == '`'
  }

  /**
   * Wrap every URL starting with `anchor` using `template`, where `{url}` is the captured URL
   * verbatim and `{enc}` is its URL-encoded form. E.g.
   * {{{
   * wrappingUrls("https://tracker.example.com",
   *              "https://fp.publisher.com/collect?dest={enc}")
   * }}}
   */
  def wrappingUrls(anchor: String, template: String): TokenRewriter =
    new TokenRewriter(
      Seq(anchor),
      token =>
        template
          .replace("{enc}", java.net.URLEncoder.encode(token, "UTF-8"))
          .replace("{url}", token)
    )
}
