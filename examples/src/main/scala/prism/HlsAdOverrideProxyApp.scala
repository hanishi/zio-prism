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
import zio.stream.{ZSink, ZStream}

import com.sun.net.httpserver.{HttpExchange, HttpHandler, HttpServer}

import java.io.InputStream
import java.net.{InetSocketAddress, URI, URLDecoder, URLEncoder}
import java.net.http.{HttpClient, HttpRequest, HttpResponse}
import java.nio.charset.StandardCharsets.UTF_8
import java.util.concurrent.Executors

import scala.util.matching.Regex

/**
 * The payoff rung: a proxy that '''overrides''' the ad in a real SSAI stream. Where `HlsProxyApp`
 * passes a stream through untouched and `HlsAdStitchApp` splices on paper, this does the actual
 * thing — it intercepts a genuinely ad-stitched stream and substitutes *its own* ad for the one the
 * upstream vendor inserted. Open the browser and you watch an ad *this server* chose, not theirs.
 *
 * The upstream is the Yospace DAI stream: encrypted premium content (`#EXT-X-KEY:METHOD=AES-128`)
 * with ad pods stitched in as unencrypted segments (`#EXT-X-KEY:METHOD=NONE`) between
 * `#EXT-X-DISCONTINUITY` markers. That key-method flip is a clean, reliable ad detector — no
 * guessing from filenames:
 *
 *   - segments governed by `METHOD=NONE`  -> the upstream's ad (to be replaced)
 *   - segments governed by `METHOD=AES-128` -> premium content (passed through, keys and all)
 *
 * `overrideAds` walks the media playlist tracking the current key method. The first time it crosses
 * into an ad break it emits *our* pod — a discontinuity-bracketed, unencrypted Big Buck Bunny clip
 * (recognizable on screen, MPEG-TS like the content, so it splices) — and then suppresses the
 * upstream's original ad segments until content resumes. Routing then re-points every surviving URL
 * (content segments, decryption keys, and our ad segments) back through this proxy, exactly as
 * `HlsProxyApp` does. The selection here is a fixed pod; swapping in `HlsAdStitchApp`'s
 * avail-index `select` (warm hit vs slate) is the natural merge of the two.
 *
 * Honest scope: our pod is a different resolution than the content, so the player re-inits across
 * the discontinuity (that is what the discontinuity is *for*) — a brief rebuffer at the splice is
 * normal. Format-matching the ad to each content rendition is the "conditioning" step real SSAI
 * does ahead of time and this example asserts rather than performs.
 *
 * Run with: `sbt "examples/runMain prism.HlsAdOverrideProxyApp"`  then open http://localhost:8088
 */
object HlsAdOverrideProxyApp extends ZIOAppDefault {

  // A real Yospace-stitched SSAI stream: content -> #EXT-X-DISCONTINUITY -> ad -> ... whose ads we
  // are about to override.
  private val upstream =
    "https://test-streams.mux.dev/dai-discontinuity-deltatre/manifest.m3u8"

  // OUR ad creative, asserted pre-conditioned: recognizable Big Buck Bunny segments, MPEG-TS like
  // the content so they splice across the discontinuity. Enough distinct 10s segments to fill a
  // long break without an obvious short loop; `fill` cycles them if a break runs longer still.
  private val ourAdPod: List[(String, Double)] =
    (846 to 865)
      .map(n => s"https://test-streams.mux.dev/x36xhzz/url_6/url_$n/193039199_mp4_h264_aac_hq_7.ts" -> 10.0)
      .toList

  private val client =
    HttpClient.newBuilder.followRedirects(HttpClient.Redirect.NORMAL).build

  private val UriAttr: Regex   = "URI=\"([^\"]*)\"".r
  private val KeyMethod: Regex = """#EXT-X-KEY:METHOD=([A-Za-z0-9-]+).*""".r

  private def parseExtinf(s: String): Double =
    s.stripPrefix("#EXTINF:").takeWhile(c => c.isDigit || c == '.').toDoubleOption.getOrElse(0.0)

  /**
   * Replace every upstream ad break with our pod, '''filled to the break's own duration'''. Detects
   * ad segments by the governing key method (`METHOD=NONE` => ad). It measures a break by summing
   * the upstream ad `#EXTINF`s, then emits our discontinuity-bracketed pod sized to that span — so
   * our ad covers the *entire* slot the vendor's ad occupied, not a fixed snippet — dropping the
   * upstream's ad segments and inner discontinuities. Content (a non-NONE key) resumes after.
   */
  private def overrideAds(manifest: String): (String, Int) = {
    val out    = scala.collection.mutable.ArrayBuffer.empty[String]
    var inAd   = false
    var adDur  = 0.0
    var breaks = 0

    def emitPod(span: Double): Unit = {
      out += "#EXT-X-DISCONTINUITY"   // content -> our ad
      out += "#EXT-X-KEY:METHOD=NONE" // our ad is unencrypted
      var filled = 0.0
      var i      = 0
      while (filled < span && ourAdPod.nonEmpty) {
        val (uri, dur) = ourAdPod(i % ourAdPod.length) // cycle the pod if the break runs long
        out += f"#EXTINF:$dur%.3f,"
        out += uri
        filled += dur
        i += 1
      }
      out += "#EXT-X-DISCONTINUITY"   // our ad -> content
    }

    manifest.linesIterator.foreach { line =>
      line.trim match {
        case KeyMethod(m) if m.equalsIgnoreCase("NONE") =>
          if (!inAd) { inAd = true; breaks += 1; adDur = 0.0 } // entering a break; measure it
        case KeyMethod(_) =>
          if (inAd) { emitPod(adDur); inAd = false } // break over: emit our pod filled to its span
          out += line
        case "#EXT-X-DISCONTINUITY" =>
          () // every discontinuity here brackets an ad; our pod supplies its own, so drop originals
        case extinf if inAd && extinf.startsWith("#EXTINF:") =>
          adDur += parseExtinf(extinf) // accumulate the upstream break's duration; don't emit
        case _ =>
          if (!inAd) out += line // pass content + header tags through; skip upstream ad segments
      }
    }
    if (inAd) emitPod(adDur) // stream ended inside a break
    (out.mkString("\n") + "\n", breaks)
  }

  /** Resolve `ref` against the playlist base and re-point it back through this proxy. */
  private def route(base: URI, ref: String): String = {
    val abs        = base.resolve(ref.trim)
    val enc        = URLEncoder.encode(abs.toString, UTF_8)
    val isPlaylist = abs.getPath.toLowerCase.endsWith(".m3u8")
    (if (isPlaylist) "/proxy?u=" else "/seg?u=") + enc
  }

  /** Re-point every URL in a playlist back through this proxy, resolving relatives against `base`. */
  private def rewriteManifest(body: String, base: URI): String =
    body.linesIterator
      .map { raw =>
        val line = raw.trim
        if (line.isEmpty) raw
        else if (line.startsWith("#"))
          if (line.contains("URI=\""))
            UriAttr.replaceAllIn(line, m => Regex.quoteReplacement(s"""URI="${route(base, m.group(1))}""""))
          else raw
        else route(base, line)
      }
      .mkString("\n") + "\n"

  private def get(u: String, range: Option[String]): HttpResponse[InputStream] = {
    val b = HttpRequest.newBuilder(URI.create(u)).GET
    range.foreach(r => b.header("Range", r))
    client.send(b.build, HttpResponse.BodyHandlers.ofInputStream)
  }

  private def queryParam(ex: HttpExchange, key: String): Option[String] =
    Option(ex.getRequestURI.getRawQuery).flatMap { q =>
      q.split("&").collectFirst {
        case kv if kv.startsWith(key + "=") => URLDecoder.decode(kv.drop(key.length + 1), UTF_8)
      }
    }

  private def send(ex: HttpExchange, status: Int, bytes: Array[Byte], contentType: String, cache: String): Unit = {
    val h = ex.getResponseHeaders
    h.set("Content-Type", contentType)
    h.set("Access-Control-Allow-Origin", "*")
    h.set("Cache-Control", cache)
    ex.sendResponseHeaders(status, bytes.length.toLong)
    val os = ex.getResponseBody
    try os.write(bytes)
    finally os.close()
  }

  private def landingPage: Array[Byte] =
    s"""<!doctype html><meta charset=utf-8><title>zio-prism ad-override proxy</title>
       |<body style="margin:0;background:#111;color:#eee;font-family:system-ui">
       |<p style="padding:.5rem 1rem;margin:0;background:#222">This proxy <b>overrides</b> the upstream SSAI ad with its own (Big Buck Bunny).</p>
       |<video id=v controls autoplay muted playsinline style="width:100%;max-height:78vh"></video>
       |<pre id=log style="padding:1rem"></pre>
       |<script src="https://cdn.jsdelivr.net/npm/hls.js@1"></script>
       |<script>
       |const src = "/proxy?u=" + encodeURIComponent(${jsString(upstream)});
       |const v = document.getElementById('v'), log = document.getElementById('log');
       |let lastCc = null;
       |const line = (t, hot) => { const d = document.createElement('div'); d.textContent = t; if (hot) d.style.color = '#ffb000'; log.prepend(d); };
       |if (window.Hls && Hls.isSupported()) {
       |  const h = new Hls(); h.loadSource(src); h.attachMedia(v);
       |  h.on(Hls.Events.FRAG_CHANGED, (_, d) => {
       |    const f = d.frag;
       |    // the played URL is /seg?u=<encoded upstream>; decode it to show the REAL segment name
       |    const m = f.url.match(/[?&]u=([^&]+)/);
       |    const real = m ? decodeURIComponent(m[1]) : f.url;
       |    const name = real.split('/').slice(-2).join('/').split('?')[0];
       |    if (lastCc !== null && f.cc !== lastCc) line("──  #EXT-X-DISCONTINUITY  (your ad splice)  ──", true);
       |    lastCc = f.cc;
       |    line("seg  cc=" + f.cc + "   " + name);
       |  });
       |} else if (v.canPlayType('application/vnd.apple.mpegurl')) { v.src = src; }
       |else { line("This browser can't play HLS."); }
       |</script>
       |""".stripMargin.getBytes(UTF_8)

  private def jsString(s: String): String = "\"" + s.replace("\\", "\\\\").replace("\"", "\\\"") + "\""

  def run =
    for {
      rt   <- ZIO.runtime[Any]
      port  = sys.env.get("PORT").flatMap(_.toIntOption).getOrElse(8088)
      _    <- ZIO.scoped {
                ZIO.acquireRelease(ZIO.attempt(start(port, rt)))(s => ZIO.succeed(s.stop(0))) *>
                  Console.printLine(s"ad-override proxy on http://localhost:$port  — open it to watch YOUR ad replace the upstream's") *>
                  Console.printLine(s"  upstream: $upstream") *>
                  Console.printLine("  Ctrl-C to stop") *>
                  ZIO.never
              }
    } yield ()

  private def start(port: Int, rt: Runtime[Any]): HttpServer = {
    val server = HttpServer.create(new InetSocketAddress(port), 0)
    server.setExecutor(Executors.newCachedThreadPool())

    server.createContext("/", handler { ex =>
      if (ex.getRequestURI.getPath == "/") send(ex, 200, landingPage, "text/html; charset=utf-8", "no-cache")
      else send(ex, 404, "not found\n".getBytes(UTF_8), "text/plain; charset=utf-8", "no-cache")
    })

    server.createContext("/proxy", handler { ex =>
      queryParam(ex, "u") match {
        case None => send(ex, 400, "missing ?u=\n".getBytes(UTF_8), "text/plain; charset=utf-8", "no-cache")
        case Some(u) =>
          val resp = get(u, None)
          val body = new String(resp.body.readAllBytes(), UTF_8)
          // A media playlist (has segments) -> override its ads first; a master playlist -> route only.
          val stitched = if (body.contains("#EXTINF")) overrideAds(body)._1 else body
          val out      = rewriteManifest(stitched, URI.create(u)).getBytes(UTF_8)
          send(ex, 200, out, "application/vnd.apple.mpegurl", "no-cache")
      }
    })

    server.createContext("/seg", handler { ex =>
      queryParam(ex, "u") match {
        case None => send(ex, 400, "missing ?u=\n".getBytes(UTF_8), "text/plain; charset=utf-8", "no-cache")
        case Some(u) =>
          val range = Option(ex.getRequestHeaders.getFirst("Range"))
          val resp  = get(u, range)
          val h     = ex.getResponseHeaders
          h.set("Content-Type", resp.headers.firstValue("content-type").orElse("application/octet-stream"))
          h.set("Access-Control-Allow-Origin", "*")
          h.set("Accept-Ranges", "bytes")
          val cr = resp.headers.firstValue("content-range")
          if (cr.isPresent) h.set("Content-Range", cr.get)
          val clen = resp.headers.firstValueAsLong("content-length").orElse(-1L)
          ex.sendResponseHeaders(resp.statusCode, if (clen > 0) clen else 0L)
          val os = ex.getResponseBody
          Unsafe.unsafe { implicit u =>
            rt.unsafe
              .run(ZStream.fromInputStream(resp.body, 64 * 1024).run(ZSink.fromOutputStream(os)))
              .getOrThrow()
          }
          os.close()
      }
    })

    server.start()
    server
  }

  private def handler(body: HttpExchange => Unit): HttpHandler =
    (ex: HttpExchange) =>
      try body(ex)
      catch {
        case t: Throwable =>
          val msg = s"proxy error: ${t.getClass.getSimpleName}: ${t.getMessage}\n".getBytes(UTF_8)
          try send(ex, 502, msg, "text/plain; charset=utf-8", "no-cache")
          catch { case _: Throwable => () }
      } finally ex.close()

}
