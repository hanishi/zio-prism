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

import zio.Chunk

import java.util.Arrays

/**
 * Applies an inner [[Rewriter]] only to the '''text content''' of an HTML stream — never to
 * tags, attributes, `<script>` / `<style>` bodies, or comments. This is the context-aware mode:
 * it makes even common words safe to rewrite, because `<head>`, `class="header"` and
 * `<script>head</script>` are all left untouched while the word "head" in visible text is
 * rewritten. Lift it into a `ZPipeline` with [[RewriteStream.htmlText]].
 *
 * A streaming, single-pass tokenizer driven one chunk at a time. The parse mode and both carries
 * (the raw markup lookahead and the inner rewriter's text carry) live in an immutable
 * [[HtmlState]] threaded through the streaming channel — so they are per-stream and survive chunk
 * boundaries; the same patterns split across packets still match within a text run. Both carries
 * are bounded: the markup lookahead holds at most a few ambiguous bytes (a possible `<!--` opener,
 * or a tag-name prefix no longer than `script`), and the text carry is bounded by the inner
 * rewriter's own contract — so memory stays constant even against adversarial markup. Patterns do
 * NOT match across a tag boundary (each text run is rewritten independently), which is the correct
 * HTML semantics — `inter<b>nal` is two text nodes.
 *
 * Scope: it understands tags (with quoted attribute values that may contain `>`),
 * `<!-- comments -->`, and `<script>`/`<style>` raw-text elements (closed by `</script` /
 * `</style`). Other constructs get browser-style fallbacks rather than their own states:
 * `<!…>` (doctype, CDATA) is consumed as markup up to the first unquoted `>` — so a
 * `<![CDATA[…]]>` whose payload contains a `>` has the rest of that payload treated as
 * rewritable text, the same place an HTML parser's "bogus comment" would end — and a `<` not
 * followed by a letter, `!` or `/` (e.g. `<?`) is literal text, fed to the inner rewriter.
 * SVG/MathML foreign content is tokenized by the ordinary HTML rules. For XML documents
 * (VAST, RSS), apply the inner rewriter directly instead of fronting it with this tokenizer.
 *
 * Output is built by concatenating a handful of `Chunk[Byte]` segments per chunk (verbatim markup
 * slices and inner-rewriter outputs), not by per-byte appends — so there is no boxing in the hot
 * path; the inner matchers still assemble their own output with `System.arraycopy`.
 */
final class HtmlTextTokenizer(inner: Rewriter) {
  import HtmlTextTokenizer.*

  /**
   * Process `state.carry ++ input`, emitting the bytes safe to release now and returning the
   * state to thread into the next call. At `atEOF` any unclassifiable tail is emitted verbatim
   * and the final text run is flushed.
   */
  def process(state: HtmlState, input: Chunk[Byte], atEOF: Boolean): (Chunk[Byte], HtmlState) = {
    val carry0 = state.carry ++ input
    val a      = carry0.toArray
    val n      = a.length

    var out          = Chunk.empty[Byte]
    var textCarry    = state.textCarry
    var mode         = state.mode
    var quote        = state.quote
    var lastNonSpace = state.lastNonSpace
    var isClosing    = state.isClosing
    var pendingRaw   = state.pendingRaw
    var rawClose     = state.rawClose
    var rawMatch     = state.rawMatch
    var dashCount    = state.dashCount

    def append(c: Chunk[Byte]): Unit = if (c.nonEmpty) out = out ++ c
    def slice(from: Int, until: Int): Chunk[Byte] =
      Chunk.fromArray(Arrays.copyOfRange(a, from, until))

    // Stream text bytes through the inner rewriter (carry kept in textCarry).
    def feedText(from: Int, until: Int): Unit = {
      textCarry = textCarry ++ slice(from, until)
      val (o, consumed) = inner(textCarry, atEOF = false)
      textCarry = textCarry.drop(consumed)
      append(o)
    }
    // End the current text run: flush the inner rewriter's remaining carry.
    def flushText(): Unit =
      if (textCarry.nonEmpty) {
        val (o, _) = inner(textCarry, atEOF = true)
        textCarry = Chunk.empty
        append(o)
      }
    // Enter TAG mode at the current '<'. We do NOT emit or advance: the TAG branch emits
    // verbatim starting from the '<' itself.
    def beginTag(closing: Boolean, raw: Array[Byte]): Unit = {
      flushText(); isClosing = closing; pendingRaw = raw; lastNonSpace = 0; quote = 0; mode = TEXT_TAG
    }

    var p    = 0
    var hold = false
    while (p < n && !hold) {
      mode match {
        case TEXT =>
          var j = p
          while (j < n && a(j) != '<') j += 1
          if (j > p) feedText(p, j)
          p = j
          if (p < n) { // a(p) == '<'
            if (p + 1 >= n) {
              if (atEOF) { flushText(); append(slice(p, p + 1)); p += 1 }
              else hold = true
            } else {
              val c1 = a(p + 1)
              if (c1 == '!') {
                if (p + 3 < n) {
                  if (a(p + 2) == '-' && a(p + 3) == '-') {
                    flushText(); append(slice(p, p + 4)); p += 4; mode = COMMENT; dashCount = 0
                  } else beginTag(closing = false, raw = NoRaw) // <!doctype …>
                } else if (atEOF) beginTag(closing = false, raw = NoRaw)
                else hold = true
              } else if (c1 == '/') {
                beginTag(closing = true, raw = NoRaw)
              } else if (isLetter(c1)) {
                var k = p + 1
                while (k < n && isLetter(a(k))) k += 1
                if (k < n || atEOF) {
                  val name = new String(a, p + 1, k - (p + 1)).toLowerCase
                  val raw  = if (name == "script") ScriptClose
                             else if (name == "style") StyleClose
                             else NoRaw
                  beginTag(closing = false, raw = raw)
                } else if (k - (p + 1) > MaxRawNameLen) {
                  // Name still running at the buffer edge, but already longer than "script" —
                  // the only names that matter for classification — so it can be treated as an
                  // ordinary tag without seeing its end. This caps the carry: an adversarial
                  // `<` + endless-letters stream never accumulates.
                  beginTag(closing = false, raw = NoRaw)
                } else hold = true
              } else {
                feedText(p, p + 1); p += 1 // '<' is literal text
              }
            }
          }

        case TEXT_TAG =>
          val segStart = p
          var done     = false
          while (p < n && !done) {
            val b = a(p)
            if (quote != 0) { if ((b & 0xff) == quote) quote = 0 }
            else if (b == '"' || b == '\'') quote = b & 0xff
            else if (b == '>') {
              val toRaw = pendingRaw.length > 0 && !isClosing && lastNonSpace != '/'
              if (toRaw) { rawClose = pendingRaw; rawMatch = 0; mode = RAW } else mode = TEXT
              pendingRaw = NoRaw; isClosing = false; lastNonSpace = 0
              done = true
            } else if (!isSpace(b)) lastNonSpace = b
            p += 1
          }
          append(slice(segStart, p))

        case COMMENT =>
          val segStart = p
          while (p < n && mode == COMMENT) {
            val b = a(p)
            if (b == '-') dashCount += 1
            else if (b == '>') { if (dashCount >= 2) mode = TEXT; dashCount = 0 }
            else dashCount = 0
            p += 1
          }
          append(slice(segStart, p))

        case RAW =>
          val segStart = p
          val r        = rawClose
          val r0       = lower(r(0))
          while (p < n && mode == RAW) {
            if (lower(a(p)) == r(rawMatch)) {
              rawMatch += 1
              if (rawMatch == r.length) {
                // matched "</script"/"</style"; consume the rest of the tag as a tag
                isClosing = true; pendingRaw = NoRaw; lastNonSpace = 0; quote = 0
                rawMatch = 0; rawClose = NoRaw; mode = TEXT_TAG
              }
            } else rawMatch = if (lower(a(p)) == r0) 1 else 0
            p += 1
          }
          append(slice(segStart, p))
      }
    }

    var newCarry = if (p >= n) Chunk.empty[Byte] else slice(p, n)
    if (atEOF) {
      if (newCarry.nonEmpty) { append(newCarry); newCarry = Chunk.empty } // unclassifiable tail → verbatim
      flushText()
    }

    val next = HtmlState(
      newCarry, textCarry, mode, quote, lastNonSpace, isClosing, pendingRaw, rawClose, rawMatch, dashCount
    )
    (out, next)
  }
}

object HtmlTextTokenizer {

  private[prism] final val TEXT     = 0
  private[prism] final val TEXT_TAG = 1
  private[prism] final val COMMENT  = 2
  private[prism] final val RAW      = 3

  private val ScriptClose = "</script".getBytes("US-ASCII")
  private val StyleClose  = "</style".getBytes("US-ASCII")
  private val NoRaw       = Array.emptyByteArray

  /** Longest raw-text element name ("script"): a tag-name lookahead past this length can be
    * classified without seeing where the name ends, which is what bounds the markup carry. */
  private val MaxRawNameLen = "script".length

  private def isLetter(b: Byte): Boolean = {
    val u = b & 0xff; (u >= 'a' && u <= 'z') || (u >= 'A' && u <= 'Z')
  }
  private def isSpace(b: Byte): Boolean =
    b == ' ' || b == '\t' || b == '\n' || b == '\r' || b == '\f'
  private def lower(b: Byte): Int = {
    val u = b & 0xff; if (u >= 'A' && u <= 'Z') u + 32 else u
  }
}

/**
 * The per-stream parse state for [[HtmlTextTokenizer]], threaded immutably through the streaming
 * channel. `carry` is the raw markup lookahead (an ambiguous `<…` at a chunk edge); `textCarry`
 * is the inner rewriter's carry for the current text run. Both keep the tokenizer correct across
 * chunk boundaries.
 */
final case class HtmlState(
    carry: Chunk[Byte],
    textCarry: Chunk[Byte],
    mode: Int,
    quote: Int,
    lastNonSpace: Int,
    isClosing: Boolean,
    pendingRaw: Array[Byte],
    rawClose: Array[Byte],
    rawMatch: Int,
    dashCount: Int
)

object HtmlState {
  val initial: HtmlState =
    HtmlState(
      Chunk.empty, Chunk.empty,
      HtmlTextTokenizer.TEXT, 0, 0, false,
      Array.emptyByteArray, Array.emptyByteArray, 0, 0
    )
}
