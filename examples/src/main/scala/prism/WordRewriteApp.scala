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

import java.io.{IOException, InputStream}
import java.net.URI
import java.net.http.{HttpClient, HttpRequest, HttpResponse}

/**
 * Whole-word rewrite over real prose, and why it differs from substring rewrite.
 *
 * `Rewrite.word` replaces a pattern only when it stands alone as a word — bounded by a non-word
 * byte (or the stream edge) on both sides. `Rewrite.literal` replaces every occurrence, including
 * the ones buried inside longer words. On natural-language text that distinction is the whole
 * game: a rule like `art -> craft` should touch the word "art", never "start", "part" or "smart".
 *
 * This pulls Peter Norvig's big.txt (~6.5 MB of public-domain English) and streams it through the
 * whole-word rewriter, never buffering the body — memory stays bounded by the longest pattern,
 * exactly like [[SampleApp]]. It then prints a one-shot contrast sentence so you can see word-mode
 * and literal-mode diverge on the same input.
 *
 * Run with: `sbt "examples/runMain prism.WordRewriteApp"`
 */
object WordRewriteApp extends ZIOAppDefault {

  private val url = "https://norvig.com/big.txt"

  // Each pattern is also a common substring of larger words (st-ART, p-ART, sm-ART; m-AN-y,
  // hu-MAN; f-ACT, re-ACT) — so word-mode and literal-mode produce visibly different output.
  private val rules = Seq(
    "art" -> "craft",
    "man" -> "person",
    "act" -> "deed"
  )
  private val word    = Rewrite.word(rules)    // whole-word: only standalone occurrences
  private val literal = Rewrite.literal(rules) // substring: every occurrence, even inside words

  private def fetch(u: String): ZIO[Any, IOException, HttpResponse[InputStream]] =
    ZIO.attemptBlockingIO {
      val client = HttpClient.newBuilder.followRedirects(HttpClient.Redirect.NORMAL).build
      client.send(HttpRequest.newBuilder(URI.create(u)).GET.build, HttpResponse.BodyHandlers.ofInputStream)
    }

  private def once(rw: Rewriter, s: String): String =
    new String(rw(Chunk.fromArray(s.getBytes("UTF-8")), atEOF = true)._1.toArray, "UTF-8")

  def run =
    for {
      _    <- Console.printLine(s"GET $url")
      resp <- fetch(url)
      clen  = resp.headers.firstValueAsLong("content-length").orElse(-1L)
      t0   <- Clock.nanoTime
      // Whole-word rewrite, streamed: bytes in -> pipeline -> bytes out, counted as they pass.
      out  <- ZStream
                .fromInputStreamZIO(ZIO.succeed(resp.body), chunkSize = 16 * 1024)
                .via(RewriteStream.pipeline(word))
                .runCount
      t1   <- Clock.nanoTime
      ms    = (t1 - t0) / 1e6
      mbps  = if (ms > 0) out.toDouble / 1e6 / (ms / 1000) else 0.0
      // Whole-word carry needs one byte past the match, so it is bounded by maxPattern + 1.
      maxP  = rules.map(_._1.length).max + 1
      _    <- Console.printLine(f"in : $clen%,d bytes (Content-Length)")
      _    <- Console.printLine(f"out: $out%,d bytes after whole-word rewrite  (delta ${out - clen}%+,d)")
      _    <- Console.printLine(f"time: $ms%,.0f ms   throughput: $mbps%,.1f MB/s   carry <= $maxP%d bytes")
      demo  = "The art of a smart start is part craft; many a human can act on fact."
      _    <- Console.printLine("")
      _    <- Console.printLine(s"input:    $demo")
      _    <- Console.printLine(s"word:     ${once(word, demo)}")
      _    <- Console.printLine(s"literal:  ${once(literal, demo)}")
    } yield ()
}
