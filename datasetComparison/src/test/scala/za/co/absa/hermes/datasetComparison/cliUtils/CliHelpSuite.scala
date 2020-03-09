package za.co.absa.hermes.datasetComparison.cliUtils

import org.scalatest.FunSuite

class CliHelpSuite extends FunSuite {

  test("Test CliHelp ToString") {
    val CHOlist = List(
      CliHelpOptions("alfa", "YES", "Why?"),
      CliHelpOptions("--help", "111", "This should help you a lot and a lot and a lot and a lot and a lot and a lot and a lot")
    )
    val CH = CliHelp(
      "MyCoolRuning tool",
      "Run this after this and tadaaaaaa",
      "The most awesome app there is in the world",
      CHOlist
    )
    val expectedResult = """MyCoolRuning tool
                   |The most awesome app there is in the world
                   |Run this after this and tadaaaaaa
                   |Options:
                   |alfa                      YES        Why?
                   |--help                    111        This should help you a lot and a lot and a lot and a lot and a lot and a lot and a lot""".stripMargin

    assert(expectedResult == CH.toString)

  }

  test("Test CliHelpOptions ToString") {
    val CHO = CliHelpOptions(
      "--help",
      "111",
      "This should help you a lot and a lot and a lot and a lot and a lot and a lot and a lot"
    )
    val expectedResult = "--help                    111        This should help you a lot and a lot and a lot and a lot and a lot and a lot and a lot"

    println(expectedResult == CHO.toString)

  }

}
