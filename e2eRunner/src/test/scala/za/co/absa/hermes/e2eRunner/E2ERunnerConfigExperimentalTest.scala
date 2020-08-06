package za.co.absa.hermes.e2eRunner

import org.scalatest.FunSuite

import scala.util.{Failure, Success}

class E2ERunnerConfigExperimentalTest extends FunSuite {
  private val randomPath = "/alfa/beta"

  test("GetCmdLineArguments - positive minimal") {
    val cmd = E2ERunnerConfigExperimental.getCmdLineArguments(Array(
      "--test-definition-path", randomPath
    )) match {
      case Success(value) => value
      case Failure(exception) => fail(exception)
    }

    assert(randomPath == cmd.testDefinition)
    assert(cmd.jarPath.isEmpty)
    assert(cmd.failfast)
  }

  test("GetCmdLineArguments - missing test definition") {
    val actualException = E2ERunnerConfigExperimental.getCmdLineArguments(Array()) match {
      case Success(_) => fail("Command line parsing passed but shouldn't")
      case Failure(exception) => exception
    }

    assert("Wrong options provided. List can be found above"  == actualException.getMessage)
  }

  test("GetCmdLineArguments - positive full") {
    val cmd = E2ERunnerConfigExperimental.getCmdLineArguments(Array(
      "--test-definition-path", randomPath,
      "--jar-path", "/some/path",
      "--fail-fast", "false"
    )) match {
      case Success(value) => value
      case Failure(exception) => fail(exception)
    }

    assert(randomPath == cmd.testDefinition)
    assert(cmd.jarPath.isDefined)
    assert("/some/path" == cmd.jarPath.get)
    assert(!cmd.failfast)
  }

}
