package za.co.absa.hermes.e2eRunner.plugins

import za.co.absa.hermes.e2eRunner.{Plugin, PluginResult}
import za.co.absa.hermes.infoFileComparison.AtumModelUtils._
import za.co.absa.hermes.infoFileComparison.InfoFileComparisonJob
import za.co.absa.hermes.infoFileComparison._

import scala.util.{Failure,Success}

case class InfoFileComparisonResult(arguments: Array[String],
                                    returnedValue: List[ModelDifference[_]],
                                    order: Int,
                                    passed: Boolean,
                                    additionalInfo: Map[String, String])
  extends PluginResult(arguments, returnedValue, order, passed, additionalInfo) {

  override def write(writeArgs: Seq[String]): Unit = {
    if (!passed) {
      val serializedData = ModelDifferenceParser.asJson(returnedValue)
      InfoFileComparisonJob.saveDataToFile(serializedData, additionalInfo("outPath"))
    }
  }

  override def logResult(): Unit = {
    if (passed) {
      scribe.info("Expected and actual _INFO files are the same.")
    } else {
      scribe.error(s"""Expected and actual info files differ.
                      |Reference path: ${additionalInfo("refPath")}
                      |Actual dataset path: ${additionalInfo("newPath")}
                      |Difference written to: ${additionalInfo("outPath")}""".stripMargin)
    }
  }
}

class InfoFileComparisonPlugin extends Plugin {
  override def name: String = "InfoFileComparison"

  override def performAction(args: Array[String], actualOrder: Int): PluginResult = {
    val parsedArgs = InfoComparisonArguments.getCmdLineArguments(args) match {
      case Success(value) => value.toStringMap
      case Failure(exception)  => throw exception
    }
    val newControlMeasure = InfoFileComparisonJob.loadControlMeasures(parsedArgs("newPath"))
    val refControlMeasure = InfoFileComparisonJob.loadControlMeasures(parsedArgs("refPath"))
    val config = InfoFileComparisonConfig.fromTypesafeConfig()

    val diff: List[ModelDifference[_]] = refControlMeasure.compareWith(newControlMeasure, config)

    InfoFileComparisonResult(args, diff, actualOrder, diff.isEmpty, parsedArgs)
  }
}