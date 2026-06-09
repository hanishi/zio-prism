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
 * End-to-end sample: pull a very large document over HTTP and rewrite it as it streams,
 * never buffering the whole body. The response is consumed as a JDK streaming
 * `InputStream` (zero extra dependencies), lifted into a `ZStream`, and piped through
 * `RewriteStream.pipeline`. Memory stays bounded by the longest pattern regardless of how
 * big the document is: the entire ~6.5 MB body flows through in 16 KB chunks.
 *
 * Run with: `sbt "runMain prism.SampleApp"`
 */
object SampleApp extends ZIOAppDefault {

  // Peter Norvig's big.txt (~6.5 MB of public-domain English prose): large, stable, plain text.
  private val url = "https://norvig.com/big.txt"

  // Independent, multi-byte patterns -> the engine dispatches to Wu-Manber. This is the
  // README's founding use case in miniature: localize content (here, British -> American
  // spelling) as it streams through a proxy.
  private val rules = Seq(
    "colour"    -> "color",
    "honour"    -> "honor",
    "recognise" -> "recognize",
    "behaviour" -> "behavior",
    "centre"    -> "center"
  )
  private val rewriter = Rewrite.literal(rules)

  private def fetch(u: String): ZIO[Any, IOException, HttpResponse[InputStream]] =
    ZIO.attemptBlockingIO {
      val client = HttpClient.newBuilder.followRedirects(HttpClient.Redirect.NORMAL).build
      client.send(HttpRequest.newBuilder(URI.create(u)).GET.build, HttpResponse.BodyHandlers.ofInputStream)
    }

  /** Apply the rewriter to a literal string (one-shot, at EOF), for the before/after demo. */
  private def rewriteOnce(s: String): String =
    new String(rewriter(Chunk.fromArray(s.getBytes("UTF-8")), atEOF = true)._1.toArray, "UTF-8")

  def run =
    for {
      _    <- Console.printLine(s"GET $url")
      resp <- fetch(url)
      clen  = resp.headers.firstValueAsLong("content-length").orElse(-1L)
      t0   <- Clock.nanoTime
      // The body never fully materializes: bytes in -> pipeline -> bytes out, counted as they pass.
      out  <- ZStream
                .fromInputStreamZIO(ZIO.succeed(resp.body), chunkSize = 16 * 1024)
                .via(RewriteStream.pipeline(rewriter))
                .runCount
      t1   <- Clock.nanoTime
      ms    = (t1 - t0) / 1e6
      mbps  = if (ms > 0) out.toDouble / 1e6 / (ms / 1000) else 0.0
      maxP  = rules.map(_._1.length).max
      _    <- Console.printLine(f"in : $clen%,d bytes (Content-Length)")
      _    <- Console.printLine(f"out: $out%,d bytes after rewrite  (delta ${out - clen}%+,d)")
      _    <- Console.printLine(f"time: $ms%,.0f ms   throughput: $mbps%,.1f MB/s   carry <= $maxP%d bytes")
      demo  = "Their colour and behaviour at the centre, we recognise the honour."
      _    <- Console.printLine("")
      _    <- Console.printLine(s"rule demo:  $demo")
      _    <- Console.printLine(s"        ->  ${rewriteOnce(demo)}")
    } yield ()
}