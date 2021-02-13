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
