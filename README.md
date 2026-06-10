# zio-prism

**PRISM**: *PRoxy Integrated Service Module*. Streaming, chunk-boundary-aware content
rewriting for **ZIO Streams**, `Chunk[Byte]`-native end to end. Like a prism refracts a
beam, it splits and transforms a byte stream as it passes through.

```scala
import prism.{Rewrite, RewriteStream}
import zio.stream.ZStream

ZStream.fromIterable(bytes)
  .via(RewriteStream.pipeline(Rewrite.literal(Seq(
    "internal.example.com" -> "public.example.com",
    "tracker.example.com"  -> "fp.example.com"
  ))))
```

Matches are found even when a pattern straddles a chunk boundary, memory is bounded by the
longest pattern (not the body), and backpressure is preserved end to end: the pipeline is
pull-based like any `ZPipeline`, never buffering ahead of the consumer.

## Where it comes from

The concept is **Greg Wilkins'**, the creator of Jetty, founder of Webtide.

At a Chinese tech company now known everywhere as a giant, its Japanese joint venture had
to sell over a B2B marketplace that had no Japanese localization, on an origin it could not
change. A local systems integrator's first attempt just parsed the whole page with regular
expressions: to a vendor who only knows web development, every problem looks like web
development. It wasn't acceptable. The real problem is harder, and streaming: rewrite the
HTTP body as it flows, correctly across chunk boundaries, without buffering. Webtide took
it on, and Greg Wilkins' answer was **jetty-prism**: a streaming Jetty proxy that
translated English to Japanese on the fly (the translation map partly in Groovy, even
strings inside JavaScript), in production until the site shipped native localization.

**zio-prism is a clean-room reimplementation of that idea on ZIO Streams**: the concept
recalled and rebuilt from scratch. It shares none of the original code; it was rebuilt from
the concept.

## Why stream the rewrite?

The original use was localization. The use that brings people here now is **first-party
proxying** — the same problem wearing a different hat.

Browsers stopped trusting third parties. Safari's **Intelligent Tracking Prevention** (ITP)
blocks third-party cookies outright; Firefox and Chrome ship comparable restrictions. So a
measurement or ad vendor that used to set a cookie on its own domain — `tracker.example.com`,
*third-party* to the site you're on — can no longer read it, and the hit goes unattributed.
The industry's answer (server-side tagging, CNAME cloaking, first-party collectors) is to send
the beacon to a domain the **publisher** controls, which forwards it server-side, where ITP has
no say.

But the URLs in the payload still point at the third-party host, so somewhere between the origin
and the client they have to be **rewritten in flight**. A video ad is a **VAST** document whose
tracking URLs sit inside `<![CDATA[…]]>`:

```
<Impression><![CDATA[https://tracker.example.com/imp?cb=1]]></Impression>
                              |  Rewrite.wrappingUrls, as it streams
                              v
<Impression><![CDATA[https://fp.publisher.com/collect?dest=https%3A%2F%2Ftracker.example.com%2Fimp%3Fcb%3D1]]></Impression>
```

`Rewrite.wrappingUrls(anchor, template)` captures each matching URL, URL-encodes it into your
first-party collector URL, and emits it; the collector unwraps `dest=` and forwards the hit
server-side (see the `UrlWrapApp` example).

You *could* buffer the whole response and rewrite it at the end. You shouldn't — and for some
inputs you can't:

|                      | buffer-and-rewrite                          | stream the rewrite                          |
| -------------------- | ------------------------------------------- | ------------------------------------------- |
| time to first byte   | after the **last** byte arrives, + rewrite  | ~immediate; receive and send overlap        |
| memory per request   | O(body) — whole body resident               | O(longest pattern) — a few bytes of carry   |
| at concurrency       | body × in-flight requests                   | a carry × in-flight requests                |
| unbounded input (live manifests, SSE) | impossible — there is no "end" to buffer | the only option that runs at all   |

The matching work is the same either way — one pass over the bytes. What buffering adds is the
entire transfer time onto first-byte latency and the entire body into memory, per request — a
gap that is invisible in a demo and fatal in production. Streaming the rewrite isn't an
optimization here; it's the design.

## Engine

`Rewriter` is the framework-agnostic carry contract:

```scala
(input: Chunk[Byte], atEOF: Boolean) => (output: Chunk[Byte], consumed: Int)
```

Emit the bytes safe to release now, and report how many leading bytes are finalized.
Anything that could still grow into a match is left unconsumed; the streaming envelope keeps
it as the "carry" and prepends it to the next chunk, so a pattern split across two chunks is
still matched. The carry never exceeds the longest pattern, so memory stays bounded no
matter how the stream is framed.

```
   chunks in                    RewriteStream.pipeline                 chunks out

      chunk  -->  buf = carry ++ chunk  -->  rewriter(buf, atEOF)
                                             = (output, consumed)  -->  output
                  carry = buf.drop(consumed)
                   `-- the unfinalized tail (< longest pattern), held for next chunk
```

`RewriteStream.pipeline(rw)` lifts a `Rewriter` into a `ZPipeline[Any, Nothing, Byte,
Byte]`. The matcher is chosen per ruleset by `Rewrite.literal`:

- one pattern -> Boyer-Moore-Horspool (skips ahead, fast on sparse patterns)
- several independent patterns (>= 2 bytes, none a substring of another) -> Wu-Manber
- otherwise -> Aho-Corasick (one O(n) pass, the correctness floor)

Output is identical across all three, verified at every chunk boundary including
mid-character splits in UTF-8; the skip matchers are chosen only when their selection
provably matches Aho-Corasick's. Because the engine is `Chunk[Byte]`-native, there is no
byte-container conversion in the pipeline.

### Match priority

When two patterns could match overlapping spans of the same input, which one wins? This
engine resolves matches **non-overlapping, left to right, by where they *end***: whichever
pattern *finishes* first, not whichever starts first or is longest. That is **not** the
POSIX leftmost-longest rule most people assume.

Aho-Corasick reports a match the moment a pattern's last byte arrives, so the earliest end
wins. Given `"bc" -> X` and `"abcd" -> Y` over the input `abcd`:

```
a b c d
  └┘        "bc"    occupies 1..2, ENDS at index 2  ← fires first
└─────┘     "abcd"  occupies 0..3, ENDS at index 3  ← bytes already taken
```

`"bc"` completes at index 2 and fires immediately; by the time `"abcd"` would complete at
index 3 its bytes are already consumed, and matches can't overlap. So `abcd` rewrites to
`aXd`: the *shorter* pattern preempts the longer one because it ended earlier. Likewise
`"aa"`/`"aaa"` over `aaaa` yields two `aa` matches, never `aaa`. The only tie-breaker: when
two patterns end at the *same* index, the longer one wins (`"cd"` and `"abcd"` both ending at
index 3 → `"abcd"`).

This quirk can only surface when one pattern is a **substring** of another, since that is the
only way two patterns can overlap on the same bytes. **Independent** rulesets (no pattern a
substring of any other, the typical case, e.g. `wikipedia.org` / `wikimedia.org` /
`wiktionary.org`) can never overlap, so the priority rule is invisible and all three matchers
produce identical output. That is exactly why the dispatcher can route independent rules to
the faster skip matchers: only the Aho-Corasick path, where substring-overlapping rules are
sent, ever exercises the earliest-ending behavior. If you need true longest-match among
overlapping rules, reorder or restructure them.

## Examples

Runnable samples live in the `examples` module (kept out of the published jar):

```
sbt "examples/runMain prism.SampleApp"                       # one-shot: a ~6.5 MB document
sbt "examples/runMain prism.WordRewriteApp"                  # whole-word vs substring, over real prose
sbt "examples/runMain prism.UrlWrapApp"                      # first-party URL wrapping in a VAST doc
sbt "examples/runMain prism.HtmlTextApp"                     # text-only rewrite: skip tags/scripts/styles
sbt "examples/runMain prism.HostRewriteApp"                  # rewrite a host in href/src values only
sbt "examples/runMain prism.StreamForeverApp"               # endless: a live, unbounded feed
sbt "examples/runMain prism.StreamForeverApp /tmp/feed.out" # ...tee the rewritten bytes to a file
```

`SampleApp` fetches a large text file, rewrites it (British → American spelling), and reports
size, throughput, and a before/after line.

`WordRewriteApp` streams the same large file through `Rewrite.word` (whole-word replace) and
prints the contrast with `Rewrite.literal`: `art -> craft` rewrites the word "art" but never
"start"/"part"/"smart", where substring replace mangles them all.

`UrlWrapApp` is the first-party-proxying use case made concrete: it feeds a VAST ad document
through `Rewrite.wrappingUrls` in deliberately tiny chunks, so every tracker URL straddles a
chunk boundary, and shows each one captured whole and re-pointed at a first-party collector
while a media URL on another host passes through untouched.

`HtmlTextApp` shows context-aware rewriting via `RewriteStream.htmlText`: a streaming HTML
tokenizer applies the inner rewriter to *visible text only*, so an aggressive whole-word rule
rewrites the body text while `<head>`, `class="header"`, comments, and `<script>`/`<style>`
bodies are left untouched — fed in 5-byte chunks to prove tags and text runs survive splitting.

`HostRewriteApp` is the mirror image: `Rewrite.replacingHost` re-points a host *only inside
`href`/`src` attribute values* (decoding `&amp;` so the host still matches in a query string),
contrasted with a plain `Rewrite.literal` swap that rewrites the host everywhere — in prose and
inside `<script>` — to show why attribute-scoped rewriting is the right tool for a proxy.

`StreamForeverApp` is the point of a streaming engine made visible: it rewrites the
**Wikimedia EventStreams** feed (a public, never-ending Server-Sent Events stream of every
wiki edit), re-pointing wiki hostnames the way a reverse proxy would. It runs until you
Ctrl-C, and prints the bytes rewritten alongside the live JVM heap, with a before -> after
example pulled from each window so you can watch the rewrite happen:

```
0.14 MB rewritten   live heap 52 MB
    e.g.  es.wikipedia.org  ->  es.wikipedia.mirror
0.53 MB rewritten   live heap 52 MB
    e.g.  en.wikinews.org  ->  en.wikinews.mirror
1.08 MB rewritten   live heap 52 MB
    e.g.  en.wikipedia.org  ->  en.wikipedia.mirror
1.38 MB rewritten   live heap 52 MB
    e.g.  ur.wikipedia.org  ->  ur.wikipedia.mirror
```

As it streams, every rewritten byte is also tee'd to a file (`rewritten.out` by default, or a
path passed as the first argument), so you keep the full rewritten output without ever holding
it in memory; the file grows until you Ctrl-C. To watch the rewrite land in real time, leave
the app running and `tail -f` the file in another terminal, and you'll see the rewritten hostnames
(`ziopedia.org`, `ziomedia.org`, …) scroll past as they're written:

```
sbt "examples/runMain prism.StreamForeverApp" &   # or run it in its own terminal
tail -f rewritten.out | grep --line-buffered zio  # watch the changed strings go by
```

The heap stays flat no matter how long it runs or how many gigabytes pass through: the only
retained state is the carry (a few bytes). Unbounded input, constant memory.

## Benchmarks

JMH throughput benchmarks live in the `bench` module (also out of the jar):

```
sbt "bench/Jmh/run .*RewriteBench.*"   # the matchers, on a ~1 MB body
sbt "bench/Jmh/run .*StreamBench.*"    # the same through the ZPipeline
```

The `Chunk[Byte]`-native matchers assemble output with `System.arraycopy` (no per-byte
boxing), so they run at full native speed: on a 10-core Apple Silicon machine, a single
sparse pattern rewrites a 1 MB body in ~0.5 ms (Boyer-Moore-Horspool), multi-pattern in
~1.2 ms (Wu-Manber), and the Aho-Corasick floor in ~4.9 ms. The `ZPipeline` adds no
measurable overhead, because there is no byte-container conversion.

## Test

```
sbt test
```

The key test runs each rewrite at **every possible chunk boundary** (sizes 1..N) and asserts
the output is identical to a single-pass reference; it also covers the matcher dispatch and
non-ASCII UTF-8, with zio-test.

## Credits

- **Concept:** Greg Wilkins (creator of Jetty, founder of Webtide), who designed the
  original **jetty-prism**, a streaming localization proxy for the Japanese joint venture of
  a now-giant Chinese tech company's B2B marketplace.
- **This reimplementation:** clean-room, from scratch, on ZIO Streams.

## License

Licensed under the [Apache License, Version 2.0](LICENSE).
