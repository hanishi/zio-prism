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
 * Minimal, allocation-light HTML character-reference decoder, scoped to what an attribute
 * *value* realistically contains (mostly `&amp;` in URL query strings). Used by
 * [[UrlAttributeRewriter]]'s `decodeEntities` option so a rule can match the logical value.
 *
 * It resolves the five predefined XML/HTML entities plus numeric references (`&#38;`, `&#x26;`);
 * anything it does not recognise is left byte-for-byte intact, so an unescaped `&` or an exotic
 * named entity is never mangled. This is deliberately *not* the full WHATWG named-character
 * reference table — front the rewriter with a real HTML parser if you need that.
 */
object HtmlEntities {

  private val named: Map[String, String] =
    Map("amp" -> "&", "lt" -> "<", "gt" -> ">", "quot" -> "\"", "apos" -> "'")

  /** Longest entity body we will look ahead for before giving up (`#x10FFFF`). */
  private val MaxBodyLen = 9

  def decode(s: String): String = {
    if (s.indexOf('&') < 0) return s // fast path: nothing to do

    val sb = new StringBuilder(s.length)
    val n  = s.length
    var i  = 0
    while (i < n) {
      val c = s.charAt(i)
      if (c == '&') {
        val semi = s.indexOf(';', i + 1)
        val resolved =
          if (semi > i && semi - (i + 1) <= MaxBodyLen) decodeOne(s.substring(i + 1, semi))
          else ""
        if (resolved.nonEmpty) { sb.append(resolved); i = semi + 1 }
        else { sb.append(c); i += 1 } // unknown / unterminated → keep the '&' literal
      } else { sb.append(c); i += 1 }
    }
    sb.toString
  }

  /** Decode one entity body (the text between `&` and `;`), or `""` if unknown. */
  private def decodeOne(body: String): String =
    if (body.isEmpty) ""
    else if (body.charAt(0) == '#') {
      try {
        val cp =
          if (body.length > 1 && (body.charAt(1) == 'x' || body.charAt(1) == 'X'))
            Integer.parseInt(body.substring(2), 16)
          else
            Integer.parseInt(body.substring(1), 10)
        if (Character.isValidCodePoint(cp)) new String(Character.toChars(cp)) else ""
      } catch { case _: NumberFormatException => "" }
    } else named.getOrElse(body, "")
}
