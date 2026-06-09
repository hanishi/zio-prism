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
import zio.Chunk

import java.nio.charset.StandardCharsets.UTF_8
import java.util.concurrent.TimeUnit
import scala.compiletime.uninitialized

/**
 * JMH throughput benchmark for the rewriting engine itself (no ZIO Streams), so the numbers
 * reflect the automaton + `Chunk[Byte]` work and nothing else. Forked JVMs and proper warmup
 * make these robust to JIT noise.
 *
 * Run all:     sbt "bench/Jmh/run .*RewriteBench.*"
 * Run quickly: sbt "bench/Jmh/run -f1 -wi 3 -i 3 -r 1 .*RewriteBench.*"
 *
 * AverageTime in microseconds per op; one op processes the whole ~1 MB body once. Divide the
 * body size by the reported µs to get MB/s.
 */
@State(Scope.Thread)
@BenchmarkMode(Array(Mode.AverageTime))
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Fork(2)
class RewriteBench {

  @Param(Array("8192"))
  var chunkSize: Int = uninitialized

  private val unit =
    """<p>The head office for the body section. Visit """ +
      """<a href="http://internal.example.com/p">internal link</a> here.</p>""" + "\n"

  var chunks: Vector[Chunk[Byte]] = uninitialized
  var literal: Rewriter           = uninitialized
  var acSingle: Rewriter          = uninitialized
  var bmhSingle: Rewriter         = uninitialized
  var wuManber: Rewriter          = uninitialized

  @Setup
  def setup(): Unit = {
    val reps  = (1 * 1024 * 1024) / unit.length
    val whole = Chunk.fromArray((unit * reps).getBytes(UTF_8))
    chunks = (whole.indices by chunkSize).map(i => whole.slice(i, math.min(i + chunkSize, whole.length))).toVector
    literal = new LiteralRewriter(Seq("internal.example.com" -> "localhost", "</head>" -> "x"))
    // Same single long pattern, two matchers: Aho-Corasick vs Boyer-Moore-Horspool.
    acSingle  = new LiteralRewriter(Seq("internal.example.com" -> "localhost"))
    bmhSingle = BmhRewriter("internal.example.com", "localhost")
    // Same multi-pattern set: Aho-Corasick (`literal`) vs Wu-Manber.
    wuManber = new WuManberRewriter(Seq("internal.example.com" -> "localhost", "</head>" -> "x"))
  }

  /** Drive a Rewriter over the chunks exactly as RewriteStream does (carry + flush). */
  private def drive(rw: Rewriter): Long = {
    var carry = Chunk.empty[Byte]
    var sink  = 0L
    var i     = 0
    while (i < chunks.length) {
      carry = carry ++ chunks(i)
      val (out, consumed) = rw(carry, false)
      carry = carry.drop(consumed)
      sink += out.length
      i += 1
    }
    val (out, _) = rw(carry, true)
    sink + out.length // returned so JMH consumes it (no dead-code elimination)
  }

  @Benchmark def literalRewriter(): Long  = drive(literal)
  @Benchmark def acSinglePattern(): Long  = drive(acSingle)
  @Benchmark def bmhSinglePattern(): Long = drive(bmhSingle)
  @Benchmark def wuManberMulti(): Long    = drive(wuManber)
}