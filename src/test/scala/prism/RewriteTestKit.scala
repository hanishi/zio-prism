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

import zio.{Chunk, ZIO}
import zio.stream.ZStream
import zio.test.*

import java.nio.charset.StandardCharsets.UTF_8

/**
 * Shared helpers for the rewriter specs: UTF-8 conversion, one-shot application, driving a
 * rewriter through the real `ZPipeline`, and the every-chunk-boundary oracle: the property
 * that streamed output is independent of how the bytes are split into chunks.
 */
object RewriteTestKit {

  def bytes(s: String): Chunk[Byte] = Chunk.fromArray(s.getBytes(UTF_8))
  def text(c: Chunk[Byte]): String  = new String(c.toArray, UTF_8)

  /** Apply `rw` once over the whole input at EOF, the non-streaming reference. */
  def oneShot(rw: Rewriter, s: String): String = text(rw(bytes(s), atEOF = true)._1)

  /** Split into fixed-size byte chunks; splits mid-character, unlike `String.grouped`. */
  private def chunkBy(c: Chunk[Byte], n: Int): Seq[Chunk[Byte]] =
    (0 until c.length by n).map(i => c.slice(i, math.min(i + n, c.length)))

  /** Drive `rw` through `RewriteStream.pipeline` over `chunks`, collecting the output. */
  def stream(rw: Rewriter, chunks: Seq[Chunk[Byte]]): ZIO[Any, Nothing, String] =
    ZStream.fromChunks(chunks*).via(RewriteStream.pipeline(rw)).runCollect.map(text)

  /** Oracle: streamed output equals the one-shot result at every chunk size 1..N. */
  def everySplitMatchesOneShot(rw: Rewriter, input: String): ZIO[Any, Nothing, TestResult] = {
    val whole    = bytes(input)
    val expected = oneShot(rw, input)
    ZIO
      .foreach(1 to whole.length)(n => stream(rw, chunkBy(whole, n)).map(got => n -> (got == expected)))
      .map(rs => assertTrue(rs.collect { case (n, false) => n }.isEmpty))
  }

  /** Oracle: two rewriters produce identical streamed output at every chunk size 1..N. */
  def everySplitAgrees(a: Rewriter, b: Rewriter, input: String): ZIO[Any, Nothing, TestResult] = {
    val whole = bytes(input)
    ZIO
      .foreach(1 to whole.length) { n =>
        val cs = chunkBy(whole, n)
        stream(a, cs).zipWith(stream(b, cs))(_ == _).map(n -> _)
      }
      .map(rs => assertTrue(rs.collect { case (n, false) => n }.isEmpty))
  }
}