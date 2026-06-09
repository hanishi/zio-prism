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

/**
 * Picks the fastest correct matcher for a set of literal `from -> to` rules, the same
 * dispatch pekko-prism uses:
 *
 *   - one pattern                          -> Boyer-Moore-Horspool
 *   - several independent patterns (>= 2 bytes, none a substring of another) -> Wu-Manber
 *   - anything else                        -> Aho-Corasick (the correctness floor)
 */
object Rewrite {

  def literal(rules: Seq[(String, String)]): Rewriter = {
    require(rules.nonEmpty, "at least one rule required")
    val froms = rules.map(_._1).toList
    if (rules.sizeIs == 1) BmhRewriter(rules.head._1, rules.head._2)
    else if (froms.forall(_.length >= 2) && independent(froms)) new WuManberRewriter(rules)
    else new LiteralRewriter(rules)
  }

  /** True if no pattern is a substring of another (the condition under which Wu-Manber's
   *  selection is identical to Aho-Corasick's). */
  private[prism] def independent(ps: List[String]): Boolean =
    ps.forall(p => ps.forall(q => p == q || !q.contains(p)))
}