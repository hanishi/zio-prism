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

import zio.ZIO
import zio.test.*

import RewriteTestKit.*

/**
 * Property-based equivalence: feed the every-chunk-boundary oracle ([[everySplitAgrees]])
 * randomized rulesets and bodies instead of hand-picked literals. Each generated case is
 * checked at *every* chunk size 1..N, so the boundary dimension stays exhaustive while the
 * (ruleset x body) dimension is now generated rather than sampled by example.
 *
 * The first three properties pin the three correctness seams the matcher dispatch rests on:
 *   - `Rewrite.literal` (the public dispatcher) == Aho-Corasick, for random independent rulesets
 *   - `WuManberRewriter` == Aho-Corasick, directly, for random independent rulesets
 *   - `BmhRewriter`     == Aho-Corasick, directly, for a random single pattern
 *
 * The last two pin the seam the *context-sensitive* rewriters rest on — that a match decision
 * which reads the byte before the match is still independent of where the chunks fall. Both had
 * fixed specs that passed while the fuzzed cases did not: the fixtures happened to avoid patterns
 * beginning on a boundary byte, and anchors landing at the very start of a mid-stream buffer.
 *
 * The alphabet mixes single-byte ASCII with a 3-byte UTF-8 char ('あ'), so generated bodies
 * routinely split mid-character — the same hazard the fixed specs cover, now fuzzed.
 */
object RewritePropertySpec extends ZIOSpecDefault {

  /** Small alphabet: dense enough that short random patterns actually occur in random
    * bodies (incl. adjacent, repeated, and boundary-straddling), plus one multi-byte char. */
  private val alphabet: Seq[Char] = Seq('a', 'b', 'c', 'd', 'あ')

  private def genStr(min: Int, max: Int): Gen[Any, String] =
    Gen.int(min, max).flatMap(n => Gen.listOfN(n)(Gen.elements(alphabet*)).map(_.mkString))

  /** Keep only patterns that are not a (proper) substring of another. The survivors are
    * pairwise independent: if kept p,q had p containing q, q would have been dropped. This is
    * exactly the precondition under which the skip matchers provably equal Aho-Corasick. */
  private def independentSubset(ps: List[String]): List[String] = {
    val distinct = ps.distinct
    distinct.filter(p => distinct.forall(q => p == q || !q.contains(p)))
  }

  private def withRepls(froms: Seq[String]): Seq[(String, String)] =
    froms.zipWithIndex.map { case (p, i) => p -> s"<$i>" }

  /** 1..5 random patterns (each >= 2 bytes, Wu-Manber-eligible), reduced to an independent
    * set and paired with distinct replacements. Always non-empty (the longest survives). */
  private val genIndependentRules: Gen[Any, Seq[(String, String)]] =
    Gen.int(1, 5).flatMap(k => Gen.listOfN(k)(genStr(2, 5))).map(ps => withRepls(independentSubset(ps)))

  /** A body rich in matches: a sequence of segments, each either an injected pattern or a
    * short random filler run. Forces real matches, adjacencies, and boundary straddles. */
  private def genBody(rules: Seq[(String, String)]): Gen[Any, String] = {
    val froms  = rules.map(_._1)
    val piece  = Gen.oneOf(Gen.elements(froms*), genStr(0, 2))
    Gen.int(0, 8).flatMap(n => Gen.listOfN(n)(piece).map(_.mkString))
  }

  private val genIndependentCase: Gen[Any, (Seq[(String, String)], String)] =
    genIndependentRules.flatMap(rules => genBody(rules).map(rules -> _))

  /** Alphabet for the whole-word property: word bytes and boundary bytes, so generated patterns
    * routinely *begin* on a boundary byte — the case where a replacement can end exactly where the
    * next match starts, leaving the carry no room for a byte of left context. */
  private val wordAlphabet: Seq[Char] = Seq('a', 'b', '-', '.', ' ')

  private def genWordStr(min: Int, max: Int): Gen[Any, String] =
    Gen.int(min, max).flatMap(n => Gen.listOfN(n)(Gen.elements(wordAlphabet*)).map(_.mkString))

  private val genWordCase: Gen[Any, (Seq[(String, String)], String)] =
    for {
      ps   <- Gen.int(1, 3).flatMap(k => Gen.listOfN(k)(genWordStr(1, 3)))
      body <- genWordStr(1, 10)
    } yield withRepls(ps.distinct.filter(_.nonEmpty)) -> body

  /** Attribute-ish fragments, mixed so anchors land at every offset — notably at index 0 of a
    * mid-stream buffer, and preceded by a name byte (`data-href`, `xhref`), where the byte that
    * decides the name boundary is no longer in the buffer. */
  private val genAttrDoc: Gen[Any, String] = {
    val frag = Gen.elements(
      "<a href=\"u/p\">", "<x data-href=\"u/p\">", "xhref=u ", "<img src='u/p'/>",
      "href", "src", "u", " ", "=", "\"", ">", "<a ", "data-"
    )
    Gen.int(1, 6).flatMap(n => Gen.listOfN(n)(frag).map(_.mkString))
  }

  private val genBmhCase: Gen[Any, (String, String, String)] =
    for {
      pat  <- genStr(1, 6)
      repl <- genStr(0, 4)
      body <- genBody(Seq(pat -> repl))
    } yield (pat, repl, body)

  def spec = suite("RewriteProperty")(
    test("Rewrite.literal (dispatcher) agrees with Aho-Corasick at every split") {
      check(genIndependentCase) { case (rules, body) =>
        everySplitAgrees(Rewrite.literal(rules), new LiteralRewriter(rules), body)
      }
    },
    test("WuManberRewriter agrees with Aho-Corasick at every split") {
      check(genIndependentCase) { case (rules, body) =>
        everySplitAgrees(new WuManberRewriter(rules), new LiteralRewriter(rules), body)
      }
    },
    test("BmhRewriter agrees with Aho-Corasick at every split (single pattern)") {
      check(genBmhCase) { case (pat, repl, body) =>
        everySplitAgrees(BmhRewriter(pat, repl), new LiteralRewriter(Seq(pat -> repl)), body)
      }
    },
    test("Rewrite.word matches its one-shot result at every split") {
      check(genWordCase) { case (rules, body) =>
        if (rules.isEmpty) ZIO.succeed(assertCompletes)
        else everySplitMatchesOneShot(Rewrite.word(rules), body)
      }
    },
    test("Rewrite.replacingHost matches its one-shot result at every split") {
      check(genAttrDoc) { doc =>
        everySplitMatchesOneShot(Rewrite.replacingHost("u", "U"), doc)
      }
    }
  ) @@ TestAspect.samples(100)
}
