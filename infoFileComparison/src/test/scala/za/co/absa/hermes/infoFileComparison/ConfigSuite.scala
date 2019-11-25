package za.co.absa.hermes.infoFileComparison

import org.scalatest.FunSuite

class ConfigSuite extends FunSuite {
  private val newPath = "/tmp/standardized_out"
  private val refPath = "/tmp/reference_data"
  private val outPath = "/tmp/out_data"

//  Others are not tested as it is almost impossible to get a proper error
//  from scopt. This is hopefully going to be implemented in final stable 4.1
  test("Happy day scenario") {
    val cmdConfig = InfoComparisonConfig.getCmdLineArguments(
      Array(
        "--new-path", newPath,
        "--ref-path", refPath,
        "--out-path", outPath
      )
    )

    assert(cmdConfig.newPath == newPath)
    assert(cmdConfig.refPath == refPath)
    assert(cmdConfig.outPath == outPath)
  }
}
