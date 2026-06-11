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

import RewriteTestKit.{bytes, text}

/**
 * Context-aware rewriting (ported from pekko-prism): the inner rewriter must touch only HTML text
 * content — never tag names, attribute values, `<script>`/`<style>` bodies, or comments — and the
 * result must be independent of how the bytes are split into chunks. The tokenizer is a streaming
 * `ZPipeline`, not a `Rewriter`, so the oracle runs at the pipeline level: every chunk size 1..N
 * must reproduce the single-shot output.
 */
object HtmlTextRewriteSpec extends ZIOSpecDefault {

  // A plain substring swap that, WITHOUT the tokenizer, would also hit tag names / attributes /
  // scripts. The tokenizer must confine it to text.
  private val inner = new LiteralRewriter(Seq("head" -> "HEAD", "internal" -> "EXTERNAL"))

  private def run(input: String, chunkSize: Int): ZIO[Any, Nothing, String] = {
    val whole  = bytes(input)
    val chunks = (0 until whole.length by chunkSize).map(i => whole.slice(i, math.min(i + chunkSize, whole.length)))
    ZStream.fromChunks(chunks*).via(RewriteStream.htmlText(inner)).runCollect.map(text)
  }

  /** Single-shot reference: the whole input as one chunk, then EOF. */
  private def oneShot(input: String): ZIO[Any, Nothing, String] =
    ZStream.fromChunk(bytes(input)).via(RewriteStream.htmlText(inner)).runCollect.map(text)

  /** Oracle: streamed output equals the one-shot result at every chunk size 1..N. */
  private def everySplitMatchesOneShot(input: String): ZIO[Any, Nothing, TestResult] =
    for {
      expected <- oneShot(input)
      results  <- ZIO.foreach(1 to bytes(input).length)(n => run(input, n).map(g => n -> (g == expected)))
    } yield assertTrue(results.collect { case (n, false) => n }.isEmpty)

  def spec = suite("HtmlTextRewrite")(
    suite("text only")(
      test("rewrites text content") {
        oneShot("<p>go to head now</p>").map(s => assertTrue(s == "<p>go to HEAD now</p>"))
      },
      test("does NOT rewrite tag names") {
        oneShot("<head><body>head</body></head>").map(s => assertTrue(s == "<head><body>HEAD</body></head>"))
      },
      test("does NOT rewrite attribute values") {
        oneShot("""<a href="internal" title="head">internal</a>""")
          .map(s => assertTrue(s == """<a href="internal" title="head">EXTERNAL</a>"""))
      },
      test("tolerates '>' inside a quoted attribute value") {
        oneShot("""<a title="a > head">head</a>""")
          .map(s => assertTrue(s == """<a title="a > head">HEAD</a>"""))
      },
      test("does NOT rewrite inside <script>") {
        oneShot("<script>var head = internal;</script>head")
          .map(s => assertTrue(s == "<script>var head = internal;</script>HEAD"))
      },
      test("does NOT rewrite inside <style>") {
        oneShot("<style>.head{}</style><p>head</p>")
          .map(s => assertTrue(s == "<style>.head{}</style><p>HEAD</p>"))
      },
      test("does NOT rewrite inside comments") {
        oneShot("<!-- head internal --><p>head</p>")
          .map(s => assertTrue(s == "<!-- head internal --><p>HEAD</p>"))
      },
      test("treats a bare '<' that is not a tag as text") {
        oneShot("1 < 2 and head").map(s => assertTrue(s == "1 < 2 and HEAD"))
      },
      test("does not match a pattern across a tag boundary") {
        oneShot("he<b>ad</b> and head").map(s => assertTrue(s == "he<b>ad</b> and HEAD"))
      }
    ),
    suite("streaming")(
      test("matches the one-shot result at every chunk boundary") {
        everySplitMatchesOneShot(doc)
      },
      test("a tag name longer than 'script' is still markup, never text — at every split") {
        val html = """<verylongcustomelement title="head">head</verylongcustomelement> head"""
        for {
          one <- oneShot(html)
          ok  <- everySplitMatchesOneShot(html)
        } yield ok &&
          assertTrue(one == """<verylongcustomelement title="head">HEAD</verylongcustomelement> HEAD""")
      },
      test("markup carry stays bounded while a pathological endless tag name streams in") {
        // `<` followed by letters forever. Before classification was possible without seeing the
        // name's end, every chunk re-held the whole run and the carry grew without limit.
        val tok    = new HtmlTextTokenizer(inner)
        val letters = bytes("a" * 64)
        val (_, maxCarry) = (1 to 64).foldLeft((tok.process(HtmlState.initial, bytes("<"), atEOF = false)._2, 0)) {
          case ((st, mx), _) =>
            val (_, next) = tok.process(st, letters, atEOF = false)
            (next, math.max(mx, next.carry.length))
        }
        assertTrue(maxCarry <= 1 + "script".length)
      },
      test("sanity: the visible text really was rewritten") {
        oneShot(doc).map(e =>
          assertTrue(
            e.contains("<title>HEAD</title>"),               // title text → rewritten
            e.contains("the HEAD office is EXTERNAL"),        // body text → rewritten
            e.contains("""class="header""""),                // attribute → untouched
            e.contains("""<script>var x = "head"; // internal</script>""") // script → untouched
          )
        )
      }
    )
  )

  private val doc =
    """<!doctype html><html><head><title>head</title></head>
      |<body class="header">
      |  <!-- head internal comment -->
      |  <p>the head office is internal</p>
      |  <a href="http://internal/head" title="head">go to head</a>
      |  <script>var x = "head"; // internal</script>
      |  <style>.head { color: internal; }</style>
      |  he<b>ad</b> head&amp;internal head.
      |</body></html>""".stripMargin
}
