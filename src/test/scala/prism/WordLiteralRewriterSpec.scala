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
 * Whole-word semantics (ported from pekko-prism) plus the every-chunk-boundary oracle: streamed
 * output must equal the one-shot result at every chunk size, including the cases where the right
 * boundary or a match's left boundary lands in a later chunk.
 */
object WordLiteralRewriterSpec extends ZIOSpecDefault {

  private val rw = new WordLiteralRewriter(Seq("head" -> "HEAD", "rate" -> "RATE"))

  def spec = suite("WordLiteralRewriter")(
    suite("single-shot")(
      test("replaces a standalone word") {
        assertTrue(oneShot(rw, "go to head now") == "go to HEAD now")
      },
      test("leaves a substring of a larger word alone") {
        assertTrue(
          oneShot(rw, "the header is ahead") == "the header is ahead",
          oneShot(rw, "headache and forehead") == "headache and forehead"
        )
      },
      test("treats punctuation as a boundary") {
        assertTrue(oneShot(rw, "(head), head. head!") == "(HEAD), HEAD. HEAD!")
      },
      test("replaces a word at the very start and end of input") {
        assertTrue(
          oneShot(rw, "head") == "HEAD",
          oneShot(rw, "head and rate") == "HEAD and RATE",
          oneShot(rw, "a rate") == "a RATE"
        )
      },
      test("does not match across digits or underscore") {
        assertTrue(oneShot(rw, "head1 _head head_") == "head1 _head head_")
      },
      test("markup is NOT protected (that is the tokenizer's job)") {
        assertTrue(oneShot(rw, "<head>") == "<HEAD>")
      }
    ),
    suite("streaming")(
      test("matches the one-shot result at every chunk boundary") {
        everySplitMatchesOneShot(
          rw,
          "head, header, ahead, head; the rate of headache vs rate. head_ _rate head"
        )
      },
      test("decides the right boundary when it arrives in a later chunk") {
        for {
          header <- stream(rw, Seq(bytes("see head"), bytes("er here")))
          word   <- stream(rw, Seq(bytes("see head"), bytes(" here")))
        } yield assertTrue(header == "see header here", word == "see HEAD here")
      },
      test("decides the left boundary when a match starts a later chunk") {
        stream(rw, Seq(bytes("go a"), bytes("head now")))
          .map(got => assertTrue(got == "go ahead now"))
      }
    )
  )
}
