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
 * A live HLS reverse proxy: stand in front of a *real* `.m3u8` stream already out on the
 * internet, re-point every URL back through this server, and stream it to a browser player.
 * Where `HlsManifestApp` rewrites a hand-made playlist, this rung proxies the genuine article
 * — so it has to handle everything real HLS does that a synthetic playlist conveniently omits.
 *
 * The default target is a real Yospace-stitched SSAI stream (Google DAI test asset): encrypted
 * premium content segments, an `#EXT-X-DISCONTINUITY`, then unencrypted ad segments — the whole
 * server-side-ad-insertion shape, live, flowing through this proxy. Open the root page and watch
 * the ad splice come through.
 *
 * What proxying a real stream forces that the synthetic one didn't:
 *
 *   1. '''Relative URLs.''' Real playlists reference segments/keys/renditions *relative* to the
 *      playlist's own location (`1041_6_1822767.ts`, `key1.json?…`, `master_1.m3u8`) — there is
 *      no host to anchor a literal swap on. Each ref is resolved against the upstream playlist's
 *      base URI (`base.resolve(ref)`) *before* being re-pointed. This is the one primitive a real
 *      proxy needs that byte-pattern rewriting (`Rewrite.literal`) does not provide.
 *   2. '''Two playlist levels.''' A master playlist's variant URLs (`.m3u8`) route back to
 *      `/proxy`, so when the player follows one it comes through here again and *its* segments are
 *      re-pointed too. Miss this and the player fetches media straight from the origin, bypassing
 *      the proxy entirely. Routing by the resolved path's extension (`.m3u8` -> `/proxy`,
 *      everything else -> `/seg`) handles master, media, audio renditions, keys, and init
 *      segments uniformly — with no per-tag special-casing.
 *   3. '''Opaque segment passthrough.''' The large media segments are never rewritten — they are
 *      streamed through byte-for-byte via `ZStream`, in constant memory, with `Range` forwarded
 *      for seeking. This is the library's thesis from the other side: the same streaming model
 *      that rewrites text leaves binary bodies untouched.
 *   4. '''CORS as a feature, not a chore.''' Many origins send no `Access-Control-Allow-Origin`,
 *      so a browser can't load them directly — but it *can* load from this proxy, which adds the
 *      header. The proxy doesn't just demonstrate rewriting; it makes an otherwise-unplayable
 *      (in-browser) stream playable.
 *
 * Two routes do all the work:
 *   - `GET /proxy?u=<absolute playlist URL>` — fetch, rewrite every URL back through this server,
 *     serve as `application/vnd.apple.mpegurl` (no-cache, since a live media playlist is re-fetched
 *     continuously).
 *   - `GET /seg?u=<absolute segment/key URL>` — stream the bytes through untouched, forwarding
 *     `Range` and propagating the upstream status/`Content-Range`.
 *
 * Dependency-free on purpose (JDK `HttpServer` + `HttpClient`), matching the other examples.
 *
 * Run with: `sbt "examples/runMain prism.HlsProxyApp"`  then open http://localhost:8088
 */
object HlsProxyApp extends ZIOAppDefault {

  // A real Yospace SSAI stream (Google DAI test asset): content -> #EXT-X-DISCONTINUITY -> ads.
  private val defaultStream =
    "https://test-streams.mux.dev/dai-discontinuity-deltatre/manifest.m3u8"

  // A real multivariant *live* stream — swap it in to watch the proxy rewrite each refresh of a
  // sliding window. (Public live endpoints come and go; this one is the question's pick.)
  private val liveStream =
    "https://cph-p2p-msl.akamaized.net/hls/live/2000341/test/master.m3u8"

  private val client =
    HttpClient.newBuilder.followRedirects(HttpClient.Redirect.NORMAL).build

  // Matches the quoted URI inside any tag that carries one (#EXT-X-KEY, #EXT-X-MAP, #EXT-X-MEDIA,
  // #EXT-X-I-FRAME-STREAM-INF, …). Non-media quoted tags (e.g. #EXT-X-YOSPACE-ANALYTICS-URL, which
  // has no `URI=` prefix) don't match, so analytics beacons are left pointing at their origin.
  private val UriAttr: Regex = "URI=\"([^\"]*)\"".r

  /** Resolve `ref` against the playlist's base and re-point it back through this proxy. A `.m3u8`
   *  target is itself a playlist (route to `/proxy` so the player keeps coming back); anything else
   *  — segment, key, init — is opaque (`/seg`). Root-relative so the browser resolves it against
   *  this server's origin, no host hardcoded. */
  private def route(base: URI, ref: String): String = {
    val abs        = base.resolve(ref.trim)
    val enc        = URLEncoder.encode(abs.toString, UTF_8)
    val isPlaylist = abs.getPath.toLowerCase.endsWith(".m3u8")
    (if (isPlaylist) "/proxy?u=" else "/seg?u=") + enc
  }

  /** Rewrite every URL in a playlist back through this proxy, resolving relatives against `base`. */
  private def rewriteManifest(body: String, base: URI): String =
    body.linesIterator
      .map { raw =>
        val line = raw.trim
        if (line.isEmpty) raw
        else if (line.startsWith("#"))
          // A tag line: re-point only its `URI="…"` value, leave the rest of the tag verbatim.
          if (line.contains("URI=\""))
            UriAttr.replaceAllIn(line, m => Regex.quoteReplacement(s"""URI="${route(base, m.group(1))}""""))
          else raw
        else route(base, line) // a bare URI line: a segment, or a variant playlist after STREAM-INF
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

  /** A landing page that points hls.js at the proxy, so opening the server just plays the stream. */
  private def landingPage(stream: String): Array[Byte] =
    s"""<!doctype html><meta charset=utf-8><title>zio-prism HLS proxy</title>
       |<body style="margin:0;background:#111;color:#eee;font-family:system-ui">
       |<video id=v controls autoplay muted playsinline style="width:100%;max-height:80vh"></video>
       |<pre id=log style="padding:1rem"></pre>
       |<script src="https://cdn.jsdelivr.net/npm/hls.js@1"></script>
       |<script>
       |const src = "/proxy?u=" + encodeURIComponent(${jsString(stream)});
       |const v = document.getElementById('v'), log = document.getElementById('log');
       |let lastCc = null;
       |const line = (t, hot) => { const d = document.createElement('div'); d.textContent = t; if (hot) d.style.color = '#ffb000'; log.prepend(d); };
       |if (window.Hls && Hls.isSupported()) {
       |  const h = new Hls(); h.loadSource(src); h.attachMedia(v);
       |  // Each ad pod is its own #EXT-X-DISCONTINUITY, so the discontinuity-sequence (frag.cc)
       |  // ticks at every splice. A change in cc IS an ad-break boundary -> flag it.
       |  h.on(Hls.Events.FRAG_CHANGED, (_, d) => {
       |    const f = d.frag, name = f.url.split('/').pop().split('?')[0];
       |    if (lastCc !== null && f.cc !== lastCc) line("──  #EXT-X-DISCONTINUITY  (splice — ad-break boundary)  ──", true);
       |    lastCc = f.cc;
       |    line("seg  cc=" + f.cc + "   " + name);
       |  });
       |} else if (v.canPlayType('application/vnd.apple.mpegurl')) { v.src = src; }
       |else { line("This browser can't play HLS."); }
       |</script>
       |""".stripMargin.getBytes(UTF_8)

  // Minimal JS string literal escaping for embedding a URL in the page.
  private def jsString(s: String): String = "\"" + s.replace("\\", "\\\\").replace("\"", "\\\"") + "\""

  def run =
    for {
      rt   <- ZIO.runtime[Any]
      port  = sys.env.get("PORT").flatMap(_.toIntOption).getOrElse(8088)
      _    <- ZIO.scoped {
                ZIO.acquireRelease(ZIO.attempt(start(port, rt)))(s => ZIO.succeed(s.stop(0))) *>
                  Console.printLine(
                    s"HLS proxy on http://localhost:$port  — open it to play the SSAI stream through the proxy"
                  ) *>
                  Console.printLine(s"  default: $defaultStream") *>
                  Console.printLine("  Ctrl-C to stop") *>
                  ZIO.never
              }
    } yield ()

  /** Build, wire, and start the server. Closures capture `rt` to run the streaming passthrough. */
  private def start(port: Int, rt: Runtime[Any]): HttpServer = {
    val server = HttpServer.create(new InetSocketAddress(port), 0)
    server.setExecutor(Executors.newCachedThreadPool())

    // Root: the player page.
    server.createContext("/", handler { ex =>
      if (ex.getRequestURI.getPath == "/") send(ex, 200, landingPage(defaultStream), "text/html; charset=utf-8", "no-cache")
      else send(ex, 404, "not found\n".getBytes(UTF_8), "text/plain; charset=utf-8", "no-cache")
    })

    // Playlist: fetch, rewrite every URL back through us, serve as an HLS manifest.
    server.createContext("/proxy", handler { ex =>
      queryParam(ex, "u") match {
        case None => send(ex, 400, "missing ?u=\n".getBytes(UTF_8), "text/plain; charset=utf-8", "no-cache")
        case Some(u) =>
          val resp = get(u, None)
          val body = new String(resp.body.readAllBytes(), UTF_8) // a manifest is small: read it whole
          val out  = rewriteManifest(body, URI.create(u)).getBytes(UTF_8)
          send(ex, 200, out, "application/vnd.apple.mpegurl", "no-cache")
      }
    })

    // Segment/key/init: stream the opaque bytes through untouched, forwarding Range.
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
          // Constant-memory passthrough: bytes in -> bytes out, never the whole segment in heap.
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

  /** Wrap a handler body so any failure becomes a 502 instead of killing the worker thread. */
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
