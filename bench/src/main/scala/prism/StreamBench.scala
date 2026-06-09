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

import org.openjdk.jmh.annotations.*
import zio.{Chunk, Runtime, Unsafe}
import zio.stream.ZStream

import java.nio.charset.StandardCharsets.UTF_8
import java.util.concurrent.TimeUnit
import scala.compiletime.uninitialized

/**
 * JMH benchmark for the end-to-end ZIO Streams path (`RewriteStream.pipeline`). Compared
 * against [[RewriteBench]] it shows the framework overhead, which is near zero: the engine
 * is `Chunk[Byte]`-native, so the `ZPipeline` adds no byte-container conversion.
 *
 * Run: sbt "bench/Jmh/run .*StreamBench.*"
 */
@State(Scope.Thread)
@BenchmarkMode(Array(Mode.AverageTime))
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 4, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Fork(1)
class StreamBench {

  @Param(Array("8192"))
  var chunkSize: Int = uninitialized

  private val unit =
    """<p>The head office for the body section. Visit """ +
      """<a href="http://internal.example.com/p">internal link</a> here.</p>""" + "\n"

  private val runtime = Runtime.default

  var chunks: Vector[Chunk[Byte]] = uninitialized
  var rw: Rewriter                = uninitialized

  @Setup
  def setup(): Unit = {
    val reps  = (1 * 1024 * 1024) / unit.length
    val whole = Chunk.fromArray((unit * reps).getBytes(UTF_8))
    chunks = (whole.indices by chunkSize).map(i => whole.slice(i, math.min(i + chunkSize, whole.length))).toVector
    rw = new WuManberRewriter(Seq("internal.example.com" -> "localhost", "</head>" -> "x"))
  }

  @Benchmark def literalPipeline(): Long =
    Unsafe.unsafe { implicit u =>
      runtime.unsafe
        .run(ZStream.fromChunks(chunks*).via(RewriteStream.pipeline(rw)).runCount)
        .getOrThrow()
    }
}