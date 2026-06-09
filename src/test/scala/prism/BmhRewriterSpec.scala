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

import zio.test.*

import RewriteTestKit.*

/** Ported from pekko-prism's BmhRewriterSpec; the every-split oracle now lives in
  * [[RewriteTestKit]] and drives the real `ZPipeline`. */
object BmhRewriterSpec extends ZIOSpecDefault {

  private val rw = BmhRewriter("internal.example.com", "www.example.com")

  def spec = suite("BmhRewriter")(
    test("replace all occurrences in one pass") {
      assertTrue(
        oneShot(rw, "a internal.example.com b internal.example.com c") ==
          "a www.example.com b www.example.com c"
      )
    },
    test("leave non-matching input untouched (zero-copy path)") {
      assertTrue(oneShot(rw, "nothing to see here") == "nothing to see here")
    },
    test("match identically at every chunk boundary (sizes 1..N)") {
      everySplitMatchesOneShot(rw, "x internal.example.com y internal.example.com z internal")
    },
    test("handle a match straddling the final boundary") {
      everySplitMatchesOneShot(rw, "prefix internal.example.com")
    },
    test("agree with LiteralRewriter (Aho-Corasick) on the same single pattern, at every split") {
      val ac = new LiteralRewriter(Seq("internal.example.com" -> "www.example.com"))
      everySplitAgrees(rw, ac, "p internal.example.com q internal.example.com r internal.exampl")
    },
    test("handle a single-byte pattern (degenerates to linear scan)") {
      assertTrue(oneShot(BmhRewriter("x", "Y"), "axbxc") == "aYbYc")
    },
    test("match non-ASCII (UTF-8 multi-byte) patterns, even split mid-character") {
      val jp = BmhRewriter("アパッチ", "APACHE")
      everySplitMatchesOneShot(jp, "x アパッチ y アパッチ z").map { everySplit =>
        assertTrue(
          oneShot(jp, "これはアパッチです、アパッチ！") == "これはAPACHEです、APACHE！",
          oneShot(BmhRewriter("Pekko", "ペッコ"), "Apache Pekko rocks") == "Apache ペッコ rocks"
        ) && everySplit
      }
    }
  )
}