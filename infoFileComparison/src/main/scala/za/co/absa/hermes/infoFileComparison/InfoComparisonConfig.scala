package za.co.absa.hermes.infoFileComparison

import scopt.OptionParser

/**
  * This is a class for configuration provided by the command line parameters
  *
  * Note: scopt requires all fields to have default values.
  *       Even if a field is mandatory it needs a default value.
  */
case class InfoComparisonConfig(newPath: String = "",
                                refPath: String = "",
                                outPath: String = "")

object InfoComparisonConfig {

  def getCmdLineArguments(args: Array[String]): InfoComparisonConfig = {
    val parser = new CmdParser("spark-submit [spark options] TestUtils.jar")

    val optionCmd = parser.parse(args, InfoComparisonConfig())
    if (optionCmd.isEmpty) {
      // Wrong arguments provided, the message is already displayed
      System.exit(1)
    }
    optionCmd.get
  }

  private class CmdParser(programName: String) extends OptionParser[InfoComparisonConfig](programName) {
    head("\n_INFO File Comparison", "")
    var newPath: Option[String] = None
    var refPath: Option[String] = None
    var outPath: Option[String] = None

    opt[String]("new-path")
      .required
      .action((value, config) => {
        newPath = Some(value)
        config.copy(newPath = value)})
      .text("Path to the new _INFO file, just generated and to be tested.")
      .validate(value =>
        if (refPath.isDefined && refPath.get.equalsIgnoreCase(value)) {
          failure("std-path and ref-path can not be equal")
        } else if (outPath.isDefined && outPath.get.equalsIgnoreCase(value)) {
          failure("std-path and out-path can not be equal")
        } else {
          success
        }
      )

    opt[String]("ref-path")
      .required
      .action((value, config) => {
        refPath = Some(value)
        config.copy(refPath = value)})
      .text("Path to supposedly correct _INFO file.")
      .validate(value =>
        if (newPath.isDefined && newPath.get.equalsIgnoreCase(value)) {
          failure("ref-path and std-path can not be equal")
        } else if (outPath.isDefined && outPath.get.equalsIgnoreCase(value)) {
          failure("ref-path and out-path can not be equal")
        } else {
          success
        }
      )

    opt[String]("out-path")
      .required
      .action((value, config) => {
        outPath = Some(value)
        config.copy(outPath = value)})
      .text("Path to where the `InfoFileComparisonJob` will save the differences in JSON format.")
      .validate(value =>
        if (newPath.isDefined && newPath.get.equalsIgnoreCase(value)) {
          failure("out-path and std-path can not be equal")
        } else if (refPath.isDefined && refPath.get.equalsIgnoreCase(value)) {
          failure("out-path and ref-path can not be equal")
        } else {
          success
        }
      )

    help("help")
      .text("prints this usage text")
  }
}

