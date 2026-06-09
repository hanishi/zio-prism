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
 * A pure, chunk-boundary-aware byte rewriter, native to ZIO `Chunk[Byte]`.
 *
 * Given the buffered `input`, produce the bytes safe to emit now and report how many
 * leading bytes were finalized. Any trailing bytes that could still grow into a match are
 * left unconsumed; the streaming envelope keeps them as the "carry" and prepends them to
 * the next chunk. The contract `(input, atEOF) => (output, consumed)` is the same one
 * pekko-prism and jetty-prism use; only the byte container and the streaming runtime differ.
 *
 * Each call is a pure function of `(input, atEOF)`: all streaming state (the carry) lives in
 * the envelope, not the instance. A `Rewriter` is therefore immutable and safe to share across
 * streams and threads. The carry it asks to retain never exceeds the longest pattern, so an
 * unbounded stream is rewritten in bounded memory.
 *
 * '''Match priority.''' Matches are non-overlapping and resolved left to right by where they
 * ''end'': when two occurrences overlap, the one that finishes first wins (and among patterns
 * ending at the same index, the longest). This is ''not'' POSIX leftmost-longest: a shorter
 * pattern embedded in a longer one can preempt it. With rules `"bc" -> X` and `"abcd" -> Y`,
 * the input `abcd` rewrites to `aXd` (`bc` finishes before `abcd` would); with `"aa"` and
 * `"aaa"`, the input `aaaa` yields two `aa` matches, never `aaa`. This only arises when one
 * pattern is a substring of another, the case [[Rewrite.literal]] routes to [[LiteralRewriter]]
 * (Aho-Corasick); independent rulesets cannot overlap this way, so all three matchers agree.
 */
trait Rewriter {
  def apply(input: Chunk[Byte], atEOF: Boolean): (Chunk[Byte], Int)
}

/**
 * Streaming multi-pattern literal find-and-replace via Aho-Corasick. Non-overlapping,
 * resolved earliest-ending (see [[Rewriter]] for the precise match-priority rule). Touches
 * every byte once; the skip matchers ([[BmhRewriter]], [[WuManberRewriter]]) are faster when
 * they apply, with [[Rewrite.literal]] dispatching.
 */
final class LiteralRewriter(
    replacements: Seq[(String, String)],
    charset: Charset = StandardCharsets.UTF_8
) extends Rewriter {

  require(replacements.nonEmpty, "at least one replacement required")

  private val patterns = replacements.map(_._1.getBytes(charset)).toVector
  private val repls     = replacements.map(_._2.getBytes(charset)).toArray
  private val ac        = AhoCorasick(patterns.map(identity))

  private val maxReplLen = repls.map(_.length).max

  def apply(input: Chunk[Byte], atEOF: Boolean): (Chunk[Byte], Int) = {
    if (input.isEmpty) return (Chunk.empty, 0)

    val bytes = input.toArray

    var out    = new Array[Byte](bytes.length + maxReplLen)
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
    var i        = 0
    while (i < bytes.length) {
      state = ac.step(state, bytes(i))
      val mlen = ac.matchLenAt(state)
      if (mlen > 0) {
        val start = i - mlen + 1
        if (start >= lastEmit) {
          if (start > lastEmit) append(bytes, lastEmit, start - lastEmit)
          val r = repls(ac.matchIdAt(state))
          append(r, 0, r.length)
          lastEmit = i + 1
          state = ac.root
        }
      }
      i += 1
    }

    val end = if (atEOF) bytes.length else math.max(lastEmit, bytes.length - ac.depthAt(state))
    if (lastEmit == 0) (input.take(end), end)
    else {
      if (lastEmit < end) append(bytes, lastEmit, end - lastEmit)
      (Chunk.fromArray(Arrays.copyOf(out, outPos)), end)
    }
  }
}