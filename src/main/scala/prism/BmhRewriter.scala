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
 * Streaming single-pattern find-and-replace using Boyer-Moore-Horspool. A bad-character
 * skip table lets the scan jump ahead, so on a long sparse pattern it examines far fewer
 * than `n` bytes. Carry never exceeds `pattern.length - 1` bytes.
 *
 * Output is assembled by `System.arraycopy` into a raw byte buffer, never by appending
 * `Chunk[Byte]` slices to a `ChunkBuilder` (which boxes every byte). The unmatched fast
 * path returns a zero-copy `input.take` slice.
 */
final class BmhRewriter(pattern: Array[Byte], replacement: Chunk[Byte]) extends Rewriter {

  require(pattern.nonEmpty, "pattern must be non-empty")

  private val m    = pattern.length
  private val last = m - 1
  private val repl = replacement.toArray
  private val skip = Array.fill(256)(m)
  locally {
    var k = 0
    while (k < last) { skip(pattern(k) & 0xff) = last - k; k += 1 }
  }

  def apply(input: Chunk[Byte], atEOF: Boolean): (Chunk[Byte], Int) = {
    if (input.isEmpty) return (Chunk.empty, 0)
    val bytes = input.toArray
    val len   = bytes.length

    var out    = new Array[Byte](len + repl.length)
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

    var lastEmit = 0
    var i        = 0
    while (i + m <= len) {
      var j = last
      while (j >= 0 && pattern(j) == bytes(i + j)) j -= 1
      if (j < 0) {
        if (i > lastEmit) append(bytes, lastEmit, i - lastEmit)
        append(repl, 0, repl.length)
        lastEmit = i + m
        i += m
      } else {
        i += skip(bytes(i + last) & 0xff)
      }
    }

    val end = if (atEOF) len else math.max(lastEmit, len - last)
    if (lastEmit == 0) (input.take(end), end)
    else {
      if (lastEmit < end) append(bytes, lastEmit, end - lastEmit)
      (Chunk.fromArray(Arrays.copyOf(out, outPos)), end)
    }
  }
}

object BmhRewriter {
  def apply(from: String, to: String, charset: Charset = StandardCharsets.UTF_8): BmhRewriter =
    new BmhRewriter(from.getBytes(charset), Chunk.fromArray(to.getBytes(charset)))
}