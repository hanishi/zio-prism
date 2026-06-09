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
 * The same chunk-boundary correctness as pekko-prism, now through a ZIO `ZPipeline` and
 * `Chunk[Byte]`-native, verified with zio-test.
 */
object RewriteStreamSpec extends ZIOSpecDefault {

  private def run(parts: Seq[String], rw: Rewriter): ZIO[Any, Nothing, String] =
    ZStream
      .fromChunks(parts.map(s => Chunk.fromArray(s.getBytes(UTF_8)))*)
      .via(RewriteStream.pipeline(rw))
      .runCollect
      .map(c => new String(c.toArray, UTF_8))

  def spec = suite("RewriteStream")(
    test("rewrite within a single chunk") {
      run(Seq("a internal.example.com b"), BmhRewriter("internal.example.com", "X"))
        .map(r => assertTrue(r == "a X b"))
    },
    test("catch a match straddling a chunk boundary (the whole point)") {
      run(Seq("...internal.examp", "le.com..."), BmhRewriter("internal.example.com", "X"))
        .map(r => assertTrue(r == "...X..."))
    },
    test("multi-pattern Wu-Manber, identical at every split (1..N)") {
      val rw   = new WuManberRewriter(Seq("internal.example.com" -> "X", "</head>" -> "Y"))
      val full = "a internal.example.com b </head> c internal.exa"
      for {
        expected <- run(Seq(full), rw)
        oks      <- ZIO.foreach(1 to full.length)(n => run(full.grouped(n).toSeq, rw).map(_ == expected))
      } yield assertTrue(oks.forall(identity))
    },
    test("dispatch: independent multi-patterns route to Wu-Manber and rewrite correctly") {
      val rw = Rewrite.literal(Seq("internal.example.com" -> "X", "tracker.example.com" -> "Y"))
      run(Seq("a internal.example.com b tracker.example.com c"), rw)
        .map(r => assertTrue(r == "a X b Y c"))
    },
    test("non-ASCII (UTF-8) patterns, split mid-character") {
      val rw = BmhRewriter("アパッチ", "APACHE")
      run(Seq("x アパッ", "チ y アパッチ"), rw).map(r => assertTrue(r == "x APACHE y APACHE"))
    }
  )
}