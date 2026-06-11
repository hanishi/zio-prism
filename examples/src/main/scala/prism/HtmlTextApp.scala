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

import zio.*
import zio.stream.ZStream

import java.nio.charset.StandardCharsets.UTF_8

/**
 * Context-aware rewriting: apply a rewriter to an HTML stream's *visible text only*, never to
 * tags, attributes, `<script>`/`<style>` bodies, or comments. `RewriteStream.htmlText` wraps the
 * inner rewriter in the streaming HTML tokenizer, so even an aggressive whole-word rule is safe —
 * `<head>`, `class="header"` and `<script>var head…</script>` are left alone while the word "head"
 * in body text is rewritten.
 *
 * The document is fed through the pipeline in deliberately tiny chunks, so tags and text runs are
 * split across chunk boundaries — yet the parse mode and the inner carry survive, so the output is
 * identical to a one-shot rewrite.
 *
 * Run with: `sbt "examples/runMain prism.HtmlTextApp"`
 */
object HtmlTextApp extends ZIOAppDefault {

  // Whole-word rewrite, applied ONLY to visible text by the tokenizer.
  private val inner = Rewrite.word(Seq("head" -> "HEAD", "internal" -> "EXTERNAL"))

  private val doc =
    """<!doctype html>
      |<html>
      |  <head><title>head office</title></head>
      |  <body class="header">
      |    <!-- head: internal note, not shown -->
      |    <p>the head office is internal</p>
      |    <a href="https://internal/head">go to head</a>
      |    <script>var head = "internal";</script>
      |    <style>.head { color: red; }</style>
      |  </body>
      |</html>""".stripMargin

  /** Stream `s` one tiny `n`-byte chunk at a time, splitting mid-tag and mid-word on purpose. */
  private def chunked(s: String, n: Int): ZStream[Any, Nothing, Byte] = {
    val b = Chunk.fromArray(s.getBytes(UTF_8))
    ZStream.fromChunks((b.indices by n).map(i => b.slice(i, math.min(i + n, b.length)))*)
  }

  def run =
    for {
      out <- chunked(doc, n = 5)
               .via(RewriteStream.htmlText(inner))
               .runCollect
               .map(c => new String(c.toArray, UTF_8))
      _ <- Console.printLine("--- input ---")
      _ <- Console.printLine(doc)
      _ <- Console.printLine("")
      _ <- Console.printLine("--- rewritten (text only, streamed in 5-byte chunks) ---")
      _ <- Console.printLine(out)
      _ <- Console.printLine("")
      _ <- Console.printLine("untouched: <head>, class=\"header\", the comment, and the <script>/<style> bodies")
      _ <- Console.printLine("rewritten: only visible text — head->HEAD, internal->EXTERNAL (whole words)")
    } yield ()
}
