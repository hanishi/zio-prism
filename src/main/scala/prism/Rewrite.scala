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
 *
 * Rules sharing the same `from` are deduplicated first (the first rule wins), so the choice
 * of matcher never changes which replacement fires.
 */
object Rewrite {

  def literal(rules: Seq[(String, String)]): Rewriter = {
    require(rules.nonEmpty, "at least one rule required")
    // Duplicate `from`s: keep the first rule. Aho-Corasick's trie naturally keeps the first,
    // but Wu-Manber builds its candidate lists by prepending, so the last would win there —
    // deduping up front keeps the dispatch below a pure speed choice, never a semantic one.
    val deduped = rules.distinctBy(_._1)
    val froms   = deduped.map(_._1).toList
    if (deduped.sizeIs == 1) BmhRewriter(deduped.head._1, deduped.head._2)
    else if (froms.forall(_.length >= 2) && independent(froms)) new WuManberRewriter(deduped)
    else new LiteralRewriter(deduped)
  }

  /**
   * Whole-word `from -> to` replace: a pattern fires only when bounded by a non-word byte (or
   * the stream edge) on both sides, so `head -> HEAD` rewrites `head` but not `header`/`ahead`.
   * A distinct operation from [[literal]] (substring replace), so it always uses
   * [[WordLiteralRewriter]] — there is no word-aware skip matcher to dispatch to.
   */
  def word(rules: Seq[(String, String)]): Rewriter = {
    require(rules.nonEmpty, "at least one rule required")
    new WordLiteralRewriter(rules)
  }

  /**
   * Capture every URL starting with `anchor` and rewrite it via `template`, where `{url}` is the
   * captured URL and `{enc}` its URL-encoded form — e.g. wrapping ad/measurement URLs behind a
   * first-party endpoint. See [[TokenRewriter.wrappingUrls]].
   */
  def wrappingUrls(anchor: String, template: String): Rewriter =
    TokenRewriter.wrappingUrls(anchor, template)

  /**
   * Rewrite `from` to `to` inside URL-bearing HTML attribute values only (`href`, `src`, …),
   * leaving tag/attribute structure intact. Matches anchor names case-insensitively and decodes
   * HTML entities in the value. See [[UrlAttributeRewriter.replacingHost]].
   */
  def replacingHost(from: String, to: String): Rewriter =
    UrlAttributeRewriter.replacingHost(from, to)

  /** True if no pattern is a substring of another (the condition under which Wu-Manber's
   *  selection is identical to Aho-Corasick's). */
  private[prism] def independent(ps: List[String]): Boolean =
    ps.forall(p => ps.forall(q => p == q || !q.contains(p)))
}