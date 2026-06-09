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

import java.nio.charset.StandardCharsets.UTF_8

/** Ported from pekko-prism's AhoCorasickSpec; the matcher is framework-agnostic, so the
  * cases are unchanged, only the assertions move to zio-test. */
object AhoCorasickSpec extends ZIOSpecDefault {

  /** Drive the automaton over `s`, collecting (endIndexInclusive, matchedPatternId). */
  private def scan(ac: AhoCorasick, s: String): List[(Int, Int)] = {
    var state = ac.root
    val acc   = List.newBuilder[(Int, Int)]
    val bytes = s.getBytes(UTF_8)
    var i     = 0
    while (i < bytes.length) {
      state = ac.step(state, bytes(i))
      if (ac.matchLenAt(state) > 0) acc += ((i, ac.matchIdAt(state)))
      i += 1
    }
    acc.result()
  }

  private def pat(ss: String*): Seq[Array[Byte]] = ss.map(_.getBytes(UTF_8))

  def spec = suite("AhoCorasick")(
    test("find classic overlapping dictionary matches") {
      // patterns: he(0) she(1) his(2) hers(3), the textbook example
      val hits = scan(AhoCorasick(pat("he", "she", "his", "hers")), "ushers")
      // "she" ends at idx 3 (longest ending there); "hers" ends at idx 5.
      assertTrue(hits.contains((3, 1)), hits.contains((5, 3)))
    },
    test("report the longest pattern when several end at the same index") {
      val hits = scan(AhoCorasick(pat("cd", "abcd")), "abcd")
      assertTrue(hits.contains((3, 1)), hits.filter(_._1 == 3).map(_._2) == List(1))
    },
    test("expose prefix depth (bytes to hold back) correctly") {
      val ac     = AhoCorasick(pat("abcd"))
      var s      = ac.root
      val depths = "abxabc".getBytes(UTF_8).map { b => s = ac.step(s, b); ac.depthAt(s) }.toList
      // a=1, ab=2, x breaks ->0, a=1, ab=2, abc=3
      assertTrue(depths == List(1, 2, 0, 1, 2, 3))
    },
    test("self-loop at root on unknown bytes") {
      assertTrue(scan(AhoCorasick(pat("xyz")), "aaaa").isEmpty)
    }
  )
}