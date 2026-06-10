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
import zio.stream.{ZChannel, ZPipeline}

/**
 * Lifts a [[Rewriter]] into a ZIO Streams `ZPipeline[Any, Nothing, Byte, Byte]`.
 *
 * The carry contract is the one from pekko-prism's `RewriteStage`: accumulate
 * `carry ++ chunk`, run the rewriter, emit the finalized output, keep the unconsumed
 * tail, and flush with `atEOF = true` when the stream ends. Because the engine is already
 * `Chunk[Byte]`-native, there is no byte-container conversion here at all.
 */
object RewriteStream {

  // Env=Any, no failures, byte chunks in and out, Unit done.
  private type Ch = ZChannel[Any, Nothing, Chunk[Byte], Any, Nothing, Chunk[Byte], Unit]

  def pipeline(rw: Rewriter): ZPipeline[Any, Nothing, Byte, Byte] =
    ZPipeline.fromChannel(loop(rw, Chunk.empty))

  /**
   * Lift a [[HtmlTextTokenizer]] (built from `inner`) into a `ZPipeline`, applying `inner` only to
   * the text content of an HTML stream — tags, attributes, `<script>`/`<style>` bodies and
   * comments pass through untouched. The tokenizer's per-stream [[HtmlState]] (parse mode + the
   * raw-markup and inner-text carries) is threaded through the loop, so it survives chunk
   * boundaries the same way the plain carry does.
   */
  def htmlText(inner: Rewriter): ZPipeline[Any, Nothing, Byte, Byte] =
    ZPipeline.fromChannel(htmlLoop(new HtmlTextTokenizer(inner), HtmlState.initial))

  /** Emit `out`, skipping the write entirely when there is nothing to flush. */
  private def emit(out: Chunk[Byte]): Ch =
    if (out.isEmpty) ZChannel.unit else ZChannel.write(out)

  private def loop(rw: Rewriter, carry: Chunk[Byte]): Ch =
    ZChannel.readWith(
      (in: Chunk[Byte]) => {
        val buf             = carry ++ in
        val (out, consumed) = rw(buf, atEOF = false)
        emit(out) *> loop(rw, buf.drop(consumed))
      },
      (err: Nothing) => ZChannel.fail(err), // input error type is Nothing: unreachable
      (_: Any) => emit(rw(carry, atEOF = true)._1)
    )

  private def htmlLoop(tok: HtmlTextTokenizer, state: HtmlState): Ch =
    ZChannel.readWith(
      (in: Chunk[Byte]) => {
        val (out, next) = tok.process(state, in, atEOF = false)
        emit(out) *> htmlLoop(tok, next)
      },
      (err: Nothing) => ZChannel.fail(err),
      (_: Any) => emit(tok.process(state, Chunk.empty, atEOF = true)._1)
    )
}