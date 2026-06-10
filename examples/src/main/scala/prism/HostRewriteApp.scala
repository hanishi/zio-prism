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
 * Attribute-value rewriting: re-point a host only where it is a URL — inside `href`/`src`
 * attribute values — and nowhere else. `Rewrite.replacingHost` does a bounded local parse of each
 * matched attribute and rewrites only the value (case-insensitive anchors, HTML entities decoded
 * so the host still matches through `&amp;`), leaving tag structure, body text, and script bodies
 * byte-for-byte intact.
 *
 * The contrast with a plain `Rewrite.literal` swap is the point: literal rewrites the host
 * *everywhere* — in prose and inside the `<script>` — which is usually wrong for a reverse proxy.
 *
 * Fed through the pipeline in tiny chunks, so attributes split across boundaries are still parsed
 * and rewritten whole.
 *
 * Run with: `sbt "examples/runMain prism.HostRewriteApp"`
 */
object HostRewriteApp extends ZIOAppDefault {

  private val attr    = Rewrite.replacingHost("internal.example.com", "cdn.public.com")
  private val literal = Rewrite.literal(Seq("internal.example.com" -> "cdn.public.com"))

  private val doc =
    """<html>
      |  <body>
      |    <p>Mirror of internal.example.com — see internal.example.com/help.</p>
      |    <a href="https://internal.example.com/p?a=1&amp;b=2">link</a>
      |    <img src="https://internal.example.com/logo.png">
      |    <script>var api = "https://internal.example.com/v1";</script>
      |  </body>
      |</html>""".stripMargin

  private def chunked(s: String, n: Int): ZStream[Any, Nothing, Byte] = {
    val b = Chunk.fromArray(s.getBytes(UTF_8))
    ZStream.fromChunks((0 until b.length by n).map(i => b.slice(i, math.min(i + n, b.length)))*)
  }

  private def runThrough(rw: Rewriter): ZIO[Any, Nothing, String] =
    chunked(doc, n = 5).via(RewriteStream.pipeline(rw)).runCollect.map(c => new String(c.toArray, UTF_8))

  def run =
    for {
      a <- runThrough(attr)
      l <- runThrough(literal)
      _ <- Console.printLine("--- input ---")
      _ <- Console.printLine(doc)
      _ <- Console.printLine("")
      _ <- Console.printLine("--- replacingHost (href/src values only, &amp; decoded) ---")
      _ <- Console.printLine(a)
      _ <- Console.printLine("")
      _ <- Console.printLine("--- literal (every occurrence, incl. prose and <script>) ---")
      _ <- Console.printLine(l)
    } yield ()
}
