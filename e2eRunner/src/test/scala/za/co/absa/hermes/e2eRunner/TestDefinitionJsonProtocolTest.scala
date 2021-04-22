/*
 * Copyright 2019 ABSA Group Limited
 *
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

package za.co.absa.hermes.e2eRunner

import org.scalatest.FunSuite

class TestDefinitionJsonProtocolTest extends FunSuite {
  private def findAllIn(s: String): Set[String] = TestDefinitionJsonProtocol.VarsPattern.findAllIn(s).toSet

  test("Test VarsPattern Regex") {
    val simpleVars =
      """"All the better to see with, my child." "#{Grandmother}#, what big teeth you have got!"
        |"All the better to eat you up with." And, saying these words, this wicked #{Wolf}#
        |fell upon Little Red #{Riding}# Hood, and ate her all up.""".stripMargin
    val simpleVarsResult = Set("Grandmother", "Wolf", "Riding")

    val varsWithUnderscores = """Alfa beta #{this_is_a_var}# gama delta"""
    val varsWithUnderscoresResult = Set("this_is_a_var")

    val varsWithSpaces = """Alfa beta #{this is a var}# gama delta"""
    val varWithSpacesResult = Set.empty[String]

    val varsWithNumbers = """Alfa beta #{this0is1a2var}# gama delta"""
    val varsWithNumbersResult = Set("this0is1a2var")

    assert(simpleVarsResult == findAllIn(simpleVars))
    assert(varsWithUnderscoresResult == findAllIn(varsWithUnderscores))
    assert(varWithSpacesResult == findAllIn(varsWithSpaces))
    assert(varsWithNumbersResult == findAllIn(varsWithNumbers))
  }

}
