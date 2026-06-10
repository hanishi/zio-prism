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
 * Whole-word streaming find/replace: like [[LiteralRewriter]], but a pattern is replaced only
 * when it is bounded by a non-word character (or the stream start/end) on BOTH sides. So
 * `head -> HEAD` rewrites the word "head" but leaves "header", "ahead" and "headache" alone.
 *
 * A "word character" is an ASCII letter, digit or underscore, plus any byte with the high bit
 * set (so accented / multibyte UTF-8 letters count as part of a word and are never split).
 * Everything else — space, punctuation, `<`, `>`, `=`, quotes — is a boundary.
 *
 * '''Important scope.''' This is `\b`-style word matching, NOT markup awareness. Because `<` and
 * `>` are boundaries, the word "head" inside `<head>` still matches (it becomes `<HEAD>`).
 * Skipping tags / attributes / scripts is the HTML tokenizer's job, not this rewriter's.
 *
 * Streaming correctness: deciding the RIGHT boundary needs one byte past the match, and deciding
 * the LEFT boundary of a match that lands at the very start of the next chunk needs one byte of
 * left context — so the carry retains up to one byte before the held region. Carry stays bounded
 * by `maxPatternLength + 1`. Output is assembled into a raw byte buffer via `System.arraycopy`
 * (never a boxing `Chunk` builder); the unmatched path returns a zero-copy `input.take` slice.
 */
final class WordLiteralRewriter(
    replacements: Seq[(String, String)],
    charset: Charset = StandardCharsets.UTF_8
) extends Rewriter {

  require(replacements.nonEmpty, "at least one replacement required")

  private val patterns   = replacements.map(_._1.getBytes(charset)).toVector
  private val repls      = replacements.map(_._2.getBytes(charset)).toArray
  private val ac         = AhoCorasick(patterns.map(identity))
  private val maxReplLen = repls.map(_.length).max

  private def isWordByte(b: Byte): Boolean = {
    val u = b & 0xff
    (u >= 'a' && u <= 'z') || (u >= 'A' && u <= 'Z') ||
    (u >= '0' && u <= '9') || u == '_' || u >= 0x80
  }
  private def isBoundary(b: Byte): Boolean = !isWordByte(b)

  def apply(input: Chunk[Byte], atEOF: Boolean): (Chunk[Byte], Int) = {
    if (input.isEmpty) return (Chunk.empty, 0)

    val bytes = input.toArray
    val len   = bytes.length

    var out    = new Array[Byte](len + maxReplLen)
    var outPos = 0
    def append(src: Array[Byte], from: Int, count: Int): Unit = {
      if (outPos + count > out.length) {
        var n = out.length * 2
        while (n < outPos + count) n *= 2
        out = Arrays.copyOf(out, n)
      }
      System.arraycopy(src, from, out, outPos, count)
      outPos += count
    }

    var state    = ac.root
    var lastEmit = 0

    // Build the (output, consumed) result for a return at `end`. When nothing has been replaced
    // yet (lastEmit still 0) the output is just the unchanged prefix — return it as a zero-copy
    // slice of the input rather than copying it through `out`.
    def finish(end: Int): (Chunk[Byte], Int) =
      if (lastEmit == 0) (input.take(end), end)
      else {
        if (lastEmit < end) append(bytes, lastEmit, end - lastEmit)
        (Chunk.fromArray(Arrays.copyOf(out, outPos)), end)
      }

    def replaceAt(start: Int, matchEnd: Int): Unit = {
      if (start > lastEmit) append(bytes, lastEmit, start - lastEmit)
      val r = repls(ac.matchIdAt(state))
      append(r, 0, r.length)
      lastEmit = matchEnd
      state = ac.root // resume fresh after the replacement
    }

    var i = 0
    while (i < len) {
      state = ac.step(state, bytes(i))
      val mlen = ac.matchLenAt(state)
      if (mlen > 0) {
        val start = i - mlen + 1
        if (start >= lastEmit) {
          // Left boundary: stream start counts as a boundary; otherwise look at the preceding
          // byte (always present — we retain one byte of context).
          val leftOK = start == 0 || isBoundary(bytes(start - 1))
          if (leftOK) {
            if (i + 1 < len) {
              if (isBoundary(bytes(i + 1))) replaceAt(start, i + 1)
              // else: a word char abuts the match → not a whole word; keep scanning
            } else if (atEOF) {
              // match ends exactly at the end of the final input → right boundary
              replaceAt(start, i + 1)
            } else {
              // match ends at the buffer edge and more bytes may come: we can't yet tell if the
              // next byte is a boundary. Hold the match (and one byte of left context) for the
              // next call.
              val keepFrom = if (start > lastEmit) start - 1 else lastEmit
              return finish(keepFrom)
            }
          }
        }
      }
      i += 1
    }

    if (atEOF) finish(len)
    else {
      // Hold back any partial match prefix at the tail (AC depth), plus one byte of left context
      // so the next call can judge a match that starts right there.
      val safe     = math.max(lastEmit, len - ac.depthAt(state))
      val keepFrom = if (safe > lastEmit) safe - 1 else lastEmit
      finish(keepFrom)
    }
  }
}
