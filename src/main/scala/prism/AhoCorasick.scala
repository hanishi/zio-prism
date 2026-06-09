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

import scala.collection.mutable

/**
 * Aho-Corasick multi-pattern matcher over raw bytes. Built once from a set of byte
 * patterns, then driven one byte at a time. The transition table is a full DFA (fail
 * links pre-resolved into `next`), so [[step]] is O(1) and never loops. Pure Scala; it
 * ports verbatim from pekko-prism (no streaming or byte-container dependency).
 *
 * The table is dense (one 256-entry `Array[Int]` per trie node), trading memory
 * (≈ total distinct pattern bytes × 256 ints) for branch-free, O(1) `step`. Fine for the
 * small literal rulesets this is built for; feeding a very large dictionary grows it.
 */
final class AhoCorasick private (
    private val next: Array[Array[Int]],
    private val depth: Array[Int],
    private val outLen: Array[Int],
    private val outId: Array[Int],
    val maxPatternLength: Int
) {
  def root: Int = 0
  def step(state: Int, b: Byte): Int = next(state)(b & 0xff)
  def depthAt(state: Int): Int       = depth(state)
  def matchLenAt(state: Int): Int    = outLen(state)
  def matchIdAt(state: Int): Int     = outId(state)
}

object AhoCorasick {

  def apply(patterns: Seq[Array[Byte]]): AhoCorasick = {
    require(patterns.nonEmpty, "at least one pattern required")
    require(patterns.forall(_.nonEmpty), "patterns must be non-empty")

    val next    = mutable.ArrayBuffer[Array[Int]](Array.fill(256)(-1))
    val depth   = mutable.ArrayBuffer[Int](0)
    val termLen = mutable.ArrayBuffer[Int](0)
    val termId  = mutable.ArrayBuffer[Int](-1)
    var maxLen  = 0

    patterns.zipWithIndex.foreach { case (pat, id) =>
      var s = 0
      for (b <- pat) {
        val ub = b & 0xff
        if (next(s)(ub) == -1) {
          next(s)(ub) = next.size
          next   += Array.fill(256)(-1)
          depth  += depth(s) + 1
          termLen += 0
          termId  += -1
        }
        s = next(s)(ub)
      }
      if (pat.length > termLen(s)) { termLen(s) = pat.length; termId(s) = id }
      maxLen = math.max(maxLen, pat.length)
    }

    val n      = next.size
    val fail   = Array.fill(n)(0)
    val outLen = Array.fill(n)(0)
    val outId  = Array.fill(n)(-1)

    val queue = mutable.Queue[Int]()
    val root  = next(0)
    var c = 0
    while (c < 256) {
      if (root(c) == -1) root(c) = 0
      else {
        fail(root(c)) = 0
        setOutput(root(c), termLen, termId, outLen, outId, fail)
        queue.enqueue(root(c))
      }
      c += 1
    }

    while (queue.nonEmpty) {
      val u = queue.dequeue()
      var ch = 0
      while (ch < 256) {
        val v = next(u)(ch)
        if (v == -1) next(u)(ch) = next(fail(u))(ch)
        else {
          fail(v) = next(fail(u))(ch)
          setOutput(v, termLen, termId, outLen, outId, fail)
          queue.enqueue(v)
        }
        ch += 1
      }
    }

    new AhoCorasick(next.toArray, depth.toArray, outLen, outId, maxLen)
  }

  private def setOutput(
      v: Int,
      termLen: mutable.ArrayBuffer[Int],
      termId: mutable.ArrayBuffer[Int],
      outLen: Array[Int],
      outId: Array[Int],
      fail: Array[Int]
  ): Unit =
    if (termLen(v) > 0) { outLen(v) = termLen(v); outId(v) = termId(v) }
    else { outLen(v) = outLen(fail(v)); outId(v) = outId(fail(v)) }
}