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

/**
 * Attribute-value rewriting (ported from pekko-prism): the transform touches only the value text
 * of matched attributes, preserving the name/`=`/quotes/delimiters byte-for-byte; plus the
 * every-chunk-boundary oracle, case-insensitive anchors, entity decoding, and overflow safety.
 */
object UrlAttributeRewriterSpec extends ZIOSpecDefault {

  // Uppercase the value so changes are obvious and structure is easy to verify.
  private val rw = new UrlAttributeRewriter(anchors = Seq("href", "src"), transform = _.toUpperCase)

  private val doc =
    """<html><head></head><body>
      |<a href="http://internal/a">one</a>
      |<img src='http://internal/b.png'/>
      |<a href=/relative/c >three</a>
      |</body></html>""".stripMargin

  def spec = suite("UrlAttributeRewriter")(
    suite("single-shot")(
      test("rewrites double-quoted values, preserving quotes") {
        assertTrue(oneShot(rw, """<a href="http://h/x">""") == """<a href="HTTP://H/X">""")
      },
      test("rewrites single-quoted values") {
        assertTrue(oneShot(rw, """<a href='http://h/x'>""") == """<a href='HTTP://H/X'>""")
      },
      test("rewrites unquoted values up to whitespace or '>'") {
        assertTrue(
          oneShot(rw, "<a href=/path/x class=z>") == "<a href=/PATH/X class=z>",
          oneShot(rw, "<a href=/path/x>") == "<a href=/PATH/X>"
        )
      },
      test("tolerates whitespace around '='") {
        assertTrue(oneShot(rw, """<a href = "y">""") == """<a href = "Y">""")
      },
      test("does not touch a bare anchor word that is not an attribute") {
        assertTrue(oneShot(rw, "see href in the docs") == "see href in the docs")
      },
      test("respects name boundaries (data-href is not href)") {
        assertTrue(oneShot(rw, """<x data-href="y">""") == """<x data-href="y">""")
      },
      test("emits verbatim (no corruption) when a quote is unterminated at EOF") {
        assertTrue(oneShot(rw, """<a href="http://unterminated""") == """<a href="http://unterminated""")
      },
      test("rewrites multiple attributes in one document") {
        assertTrue(oneShot(rw, """<a href="u1"><img src='u2'>""") == """<a href="U1"><img src='U2'>""")
      }
    ),
    suite("streaming")(
      test("matches the one-shot result at every chunk boundary") {
        everySplitMatchesOneShot(rw, doc)
      },
      test("transforms a value whose closing quote arrives in a later chunk") {
        stream(rw, Seq(bytes("""<a href="http://int"""), bytes("""ernal/x">end""")))
          .map(got => assertTrue(got == """<a href="HTTP://INTERNAL/X">end"""))
      }
    ),
    suite("case-insensitive anchors")(
      test("matches anchors regardless of case, folding only the match") {
        val ci = new UrlAttributeRewriter(anchors = Seq("href"), transform = _.toUpperCase, caseInsensitive = true)
        assertTrue(
          oneShot(ci, """<a HREF="http://h/x">""") == """<a HREF="HTTP://H/X">""",
          oneShot(ci, """<a Href='abc'>""") == """<a Href='ABC'>""",
          oneShot(ci, """<x data-HREF="y">""") == """<x data-HREF="y">"""
        )
      }
    ),
    suite("entity decoding (replacingHost)")(
      test("sees through &amp; so the host is matched in a query string") {
        val rh = UrlAttributeRewriter.replacingHost("internal.example.com", "localhost")
        assertTrue(
          oneShot(rh, """<a href="http://internal.example.com/p?a=1&amp;b=2">""") ==
            """<a href="http://localhost/p?a=1&b=2">"""
        )
      },
      test("decodes numeric references too") {
        val rh = UrlAttributeRewriter.replacingHost("internal.example.com", "localhost")
        assertTrue(
          oneShot(rh, """<a href="http://internal&#46;example&#46;com/x">""") ==
            """<a href="http://localhost/x">"""
        )
      },
      test("preserves the original encoding when nothing is replaced") {
        val rh = UrlAttributeRewriter.replacingHost("internal.example.com", "localhost")
        assertTrue(
          oneShot(rh, """<a href="http://other.test/p?a=1&amp;b=2">""") ==
            """<a href="http://other.test/p?a=1&amp;b=2">"""
        )
      }
    ),
    suite("overflow safety")(
      test("stops scanning and emits verbatim past the value-length budget") {
        val small = new UrlAttributeRewriter(Seq("href"), transform = _.toUpperCase, maxValueLength = 16)
        val in    = s"""<a href="${"x" * 100}">"""
        assertTrue(oneShot(small, in) == in)
      }
    )
  )
}
