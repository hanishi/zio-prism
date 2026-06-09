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
 * Streaming multi-pattern find-and-replace using Wu-Manber: Boyer-Moore-style skipping
 * over a set of patterns, via a 2-byte-block bad-shift table. Skip distance is bounded by
 * the shortest pattern; every pattern must be at least 2 bytes. Semantics match
 * [[LiteralRewriter]] when no pattern is a substring of another. Carry never exceeds
 * `maxPatternLength - 1`.
 */
final class WuManberRewriter(
    replacements: Seq[(String, String)],
    charset: Charset = StandardCharsets.UTF_8
) extends Rewriter {

  require(replacements.nonEmpty, "at least one replacement required")

  private val patterns = replacements.map(_._1.getBytes(charset)).toArray
  private val repls     = replacements.map(_._2.getBytes(charset)).toArray
  private val r         = patterns.length
  private val m         = patterns.map(_.length).min
  private val maxLen    = patterns.map(_.length).max
  private val maxReplLen = repls.map(_.length).max
  require(m >= 2, "Wu-Manber (block size 2) needs every pattern to be at least 2 bytes")

  private val B     = 2
  private val SIZE  = 1 << 16
  private val shift = Array.fill(SIZE)(m - B + 1)
  private val cand  = Array.fill(SIZE)(List.empty[Int])

  locally {
    var p = 0
    while (p < r) {
      val pat = patterns(p)
      var q = 0
      while (q <= m - B) {
        val h = (pat(q) & 0xff) << 8 | (pat(q + 1) & 0xff)
        val s = m - B - q
        if (s < shift(h)) shift(h) = s
        if (s == 0) cand(h) = p :: cand(h)
        q += 1
      }
      p += 1
    }
  }

  private def matchesAt(bytes: Array[Byte], start: Int, pat: Array[Byte]): Boolean = {
    var k = 0
    while (k < pat.length) { if (bytes(start + k) != pat(k)) return false; k += 1 }
    true
  }

  def apply(input: Chunk[Byte], atEOF: Boolean): (Chunk[Byte], Int) = {
    if (input.isEmpty) return (Chunk.empty, 0)
    val bytes = input.toArray
    val len   = bytes.length
    if (len < m) return if (atEOF) (input, len) else (Chunk.empty, 0)

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

    var lastEmit    = 0
    val commitLimit = if (atEOF) len - m + 1 else len - maxLen + 1
    var i           = m - 1
    while (i < len) {
      val h = (bytes(i - 1) & 0xff) << 8 | (bytes(i) & 0xff)
      val s = shift(h)
      if (s > 0) i += s
      else {
        val start = i - m + 1
        if (start >= commitLimit) i = len
        else if (start < lastEmit) i += 1
        else {
          var bestLen = -1
          var bestId  = -1
          var cs      = cand(h)
          while (cs.nonEmpty) {
            val pid = cs.head
            val pat = patterns(pid)
            if (start + pat.length <= len && pat.length > bestLen && matchesAt(bytes, start, pat)) {
              bestLen = pat.length; bestId = pid
            }
            cs = cs.tail
          }
          if (bestId >= 0) {
            if (start > lastEmit) append(bytes, lastEmit, start - lastEmit)
            val rep = repls(bestId)
            append(rep, 0, rep.length)
            lastEmit = start + bestLen
            i = lastEmit + m - 1
          } else i += 1
        }
      }
    }

    val end = if (atEOF) len else math.max(lastEmit, len - maxLen + 1)
    if (lastEmit == 0) (input.take(end), end)
    else {
      if (lastEmit < end) append(bytes, lastEmit, end - lastEmit)
      (Chunk.fromArray(Arrays.copyOf(out, outPos)), end)
    }
  }
}