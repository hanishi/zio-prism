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

/** Ported from pekko-prism's WuManberRewriterSpec; the cross-check against
  * [[LiteralRewriter]] (Aho-Corasick) at every split is the key correctness anchor. */
object WuManberRewriterSpec extends ZIOSpecDefault {

  private val rw = new WuManberRewriter(Seq("internal.example.com" -> "X", "</head>" -> "Y"))

  def spec = suite("WuManberRewriter")(
    test("replace multiple patterns in one pass") {
      assertTrue(
        oneShot(rw, "a internal.example.com b </head> c internal.example.com") == "a X b Y c X"
      )
    },
    test("leave non-matching input untouched") {
      assertTrue(oneShot(rw, "nothing here at all") == "nothing here at all")
    },
    test("be chunk-independent: every split (1..N) equals the single-pass result") {
      everySplitMatchesOneShot(rw, "p internal.example.com q </head> r internal.exa</head")
    },
    test("agree with LiteralRewriter (Aho-Corasick) on independent patterns, at every split") {
      val rules = Seq("internal.example.com" -> "X", "</head>" -> "Y", "body" -> "B")
      everySplitAgrees(
        new WuManberRewriter(rules),
        new LiteralRewriter(rules),
        "the body has internal.example.com and </head> and body again, internal."
      )
    },
    test("pick the longest pattern at a start (longest-match-wins)") {
      assertTrue(oneShot(new WuManberRewriter(Seq("ab" -> "1", "abcd" -> "2")), "Xabcd Yab") == "X2 Y1")
    },
    test("handle non-ASCII (UTF-8) patterns at every split") {
      everySplitMatchesOneShot(
        new WuManberRewriter(Seq("アパッチ" -> "A", "ペッコ" -> "P")),
        "これはアパッチとペッコです、アパッチ"
      )
    }
  )
}