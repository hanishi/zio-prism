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
 * Capture-and-transform semantics plus the every-chunk-boundary
 * oracle, including the case where a token's boundary arrives only in a later chunk.
 */
object TokenRewriterSpec extends ZIOSpecDefault {

  private val upper = new TokenRewriter(Seq("https://"), transform = _.toUpperCase)
  private val wrap = TokenRewriter.wrappingUrls(
    "https://tracker.example.com",
    "https://fp.publisher.com/collect?dest={enc}"
  )

  private def enc(s: String): String = java.net.URLEncoder.encode(s, "UTF-8")

  // A custom boundary, nothing like the default URL set: a token ends at the first byte that is
  // NOT a username character (ASCII letter, digit or underscore). Anchored on '@', this linkifies
  // @mentions — the boundary char (space, punctuation) is naturally preserved after the name.
  private val isNameByte: Byte => Boolean = b => {
    val u = b & 0xff
    (u >= 'a' && u <= 'z') || (u >= 'A' && u <= 'Z') || (u >= '0' && u <= '9') || u == '_'
  }
  private val mention = new TokenRewriter(
    anchors = Seq("@"),
    transform = tok => s"""<a href="/u/${tok.drop(1)}">$tok</a>""", // tok includes the leading '@'
    isBoundary = b => !isNameByte(b)
  )

  def spec = suite("TokenRewriter")(
    suite("capture")(
      test("captures a URL and transforms it, stopping at the boundary") {
        assertTrue(oneShot(upper, "see https://h/x and stop") == "see HTTPS://H/X and stop")
      },
      test("stops at a CDATA close bracket") {
        assertTrue(oneShot(upper, "<![CDATA[https://h/x]]>") == "<![CDATA[HTTPS://H/X]]>")
      },
      test("stops at a quote") {
        assertTrue(oneShot(upper, """src="https://h/x"""") == """src="HTTPS://H/X"""")
      },
      test("captures to EOF when there is no boundary") {
        assertTrue(oneShot(upper, "tail https://h/x") == "tail HTTPS://H/X")
      }
    ),
    suite("wrappingUrls")(
      test("embeds the URL-encoded original into a first-party URL") {
        assertTrue(
          oneShot(wrap, "<![CDATA[https://tracker.example.com/imp?cb=1]]>") ==
            "<![CDATA[https://fp.publisher.com/collect?dest=" +
            enc("https://tracker.example.com/imp?cb=1") + "]]>"
        )
      },
      test("leaves non-matching URLs alone") {
        assertTrue(
          oneShot(wrap, "<![CDATA[https://cdn.vendor.com/ad.mp4]]>") ==
            "<![CDATA[https://cdn.vendor.com/ad.mp4]]>"
        )
      }
    ),
    suite("streaming")(
      test("matches the one-shot result at every chunk boundary") {
        val rw = TokenRewriter.wrappingUrls("https://t.example", "https://fp/c?d={enc}")
        everySplitMatchesOneShot(
          rw,
          "<VAST><Impression><![CDATA[https://t.example/a?x=1]]></Impression>" +
            "<Tracking><![CDATA[https://t.example/b]]></Tracking>" +
            "<MediaFile><![CDATA[https://other/v.mp4]]></MediaFile></VAST>"
        )
      },
      test("captures a URL whose boundary arrives in a later chunk") {
        val rw = TokenRewriter.wrappingUrls("https://t.example", "https://fp/c?d={enc}")
        stream(rw, Seq(bytes("x https://t.example/a"), bytes("bc]end")))
          .map(got => assertTrue(got == "x https://fp/c?d=" + enc("https://t.example/abc") + "]end"))
      }
    ),
    suite("custom boundary (mention linkify)")(
      test("ends a token at the first non-name byte, preserving that byte") {
        assertTrue(
          oneShot(mention, "ping @carol!") == """ping <a href="/u/carol">@carol</a>!""",
          oneShot(mention, "@bob_dev, hi") == """<a href="/u/bob_dev">@bob_dev</a>, hi"""
        )
      },
      test("links several mentions and captures one running to EOF") {
        assertTrue(
          oneShot(mention, "hey @alice and @zoe") ==
            """hey <a href="/u/alice">@alice</a> and <a href="/u/zoe">@zoe</a>"""
        )
      },
      test("matches the one-shot result at every chunk boundary") {
        everySplitMatchesOneShot(mention, "cc @alice, @bob_dev & @carol! ok @zoe")
      }
    )
  )
}
