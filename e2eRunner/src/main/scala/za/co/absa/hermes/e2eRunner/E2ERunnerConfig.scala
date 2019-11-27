package za.co.absa.hermes.e2eRunner

import scopt.OptionParser

import scala.util.matching.Regex

/**
  * This is a class for configuration provided by the command line parameters
  *
  * Note: scopt requires all fields to have default values.
  *       Even if a field is mandatory it needs a default value.
  */
case class E2ERunnerConfig(rawFormat: String = "parquet",
                           rowTag: Option[String] = None,
                           csvDelimiter: String = ",",
                           csvHeader: Boolean = false,
                           keys: Option[Seq[String]] = None,
                           menasCredentialsFile: Option[String] = None,
                           menasAuthKeytab: Option[String] = None,
                           datasetName: String = "",
                           datasetVersion: String = "",
                           reportDate: String = "",
                           reportVersion: String = "") {

  private lazy val reportDateArr = reportDate.split("-")

  private def getAuthMethod: String = {
    if (menasCredentialsFile.isDefined && menasAuthKeytab.isEmpty) {
      s"--menas-credentials-file ${menasCredentialsFile.get}"
    } else if (menasCredentialsFile.isEmpty && menasAuthKeytab.isDefined) {
      s"--menas-auth-keytab ${menasAuthKeytab.get}"
    } else {
      //TODO
      throw new IllegalArgumentException
    }
  }

  private def getXMLParameters: String = {
    if (rowTag.isDefined) {
      s"--raw-format xml --row-tag $rowTag"
    } else {
      //TODO
      throw new IllegalArgumentException
    }
  }

  private def getFormat: String = {
    rawFormat.toLowerCase() match {
      case "parquet" => "--raw-format parquet"
      case "json" => "--raw-format json"
      case "csv" => s"--raw-format csv --delimiter $csvDelimiter --header $csvHeader"
      case "xml" => getXMLParameters
    }
  }

  private def getDCEParameters: String =
    s"""
      |--dataset-name $datasetName
      | --dataset-version $datasetVersion
      | --report-version $reportVersion
      | --report-date $reportDate
      | $getAuthMethod
      |""".stripMargin.filter(_ >= ' ')

  def getStdParams: String = s"$getDCEParameters $getFormat"

  def getConfParams: String = s"$getDCEParameters --experimental-mapping-rule true --catalyst-workaround true"

  def getDatasetMap: Map[String, String] = Map[String, String](
    "datasetName" -> datasetName,
    "datasetVersion" -> datasetVersion,
    "reportYear" -> reportDateArr(0),
    "reportMonth" -> reportDateArr(1),
    "reportDay" -> reportDateArr(2),
    "reportVersion" -> reportVersion
  )

}

object E2ERunnerConfig {

  /**
    * Parses and validates an Array of input parameters and creates an instance of CmdConfig case class
    * @param args Array of argument to be parsed
    * @return Returns a CmdConfig instance holding parameters passed
    */
  def getCmdLineArguments(args: Array[String]): E2ERunnerConfig = {
    val parser = new CmdParser("spark-submit [spark options] TestUtils.jar")

    parser.parse(args, E2ERunnerConfig()) match {
      case Some(config) => config
      case _            => throw new IllegalArgumentException("Wrong options provided. List can be found above")
    }
  }

  private class CmdParser(programName: String) extends OptionParser[E2ERunnerConfig](programName) {
    head("\nEnd2End Test Runner", "")
    var rawFormat: Option[String] = None

    opt[String]('f', "raw-format")
      .required
      .action((value, config) => {
        rawFormat = Some(value)
        config.copy(rawFormat = value)
      })
      .text("format of the raw data (csv, xml, parquet,fixed-width, etc.)")

    opt[String]("row-tag")
      .optional
      .action((value, config) => config.copy(rowTag = Some(value)))
      .text("use the specific row tag instead of 'ROW' for XML format")
      .validate(value =>
        if (rawFormat.isDefined && rawFormat.get.equalsIgnoreCase("xml")) {
          success
        } else {
          failure("The --row-tag option is supported only for XML raw data format")
        }
      )

    opt[String]("delimiter")
      .optional
      .action((value, config) => config.copy(csvDelimiter = value))
      .text("use the specific delimiter instead of ',' for CSV format")
      .validate(value =>
        if (rawFormat.isDefined && rawFormat.get.equalsIgnoreCase("csv")) {
          success
        } else {
          failure("The --delimiter option is supported only for CSV raw data format")
        }
      )
    // no need for validation for boolean since scopt itself will do
    opt[Boolean]("header")
      .optional
      .action((value, config) => config.copy(csvHeader = value))
      .text("use the header option to consider CSV header")
      .validate(value =>
        if (rawFormat.isDefined && rawFormat.get.equalsIgnoreCase("csv")) {
          success
        } else {
          failure("The --header option is supported only for CSV ")
        }
      )

    opt[String]("keys")
      .required
      .action((value, config) => config.copy(keys = Some(value.split(",").toSeq)))
      .text("""Unique key(s) of the dataset. Keys should be specified one by one, with "," (comma) between them.""")

    private var credsFile: Option[String] = None
    private var keytabFile: Option[String] = None
    opt[String]("menas-credentials-file")
      .optional
      .action((value, config) => {
        credsFile = Some(value)
        config.copy(menasCredentialsFile = Some(value))
      })
      .text("Path to Menas credentials config file. Suitable only for client mode")
      .validate(path =>
        if (keytabFile.isDefined) { failure("Only one authentication method is allow at a time") }
        else { success }
      )

    opt[String]("menas-auth-keytab")
      .optional
      .action((value, config) => {
        keytabFile = Some(value)
        config.copy(menasCredentialsFile = Some(value))
      })
      .text("Path to Menas Keytab file.")
      .validate(path =>
        if (credsFile.isDefined) { failure("Only one authentication method is allowed at a time") }
        else { success }
      )

    opt[String]("dataset-name")
      .required
      .action((value, config) => config.copy(datasetName = value))
      .text("Dataset name")

    opt[String]("dataset-version")
      .required
      .action((value, config) => config.copy(datasetVersion = value))
      .text("Dataset version")
      .validate(value =>
        if (value.toInt > 0) { success }
        else { failure("Option --dataset-version must be >0") }
      )

    val reportDateMatcher: Regex = "^\\d{4}-\\d{2}-\\d{2}$".r
    opt[String]("report-date")
      .required
      .action((value, config) => config.copy(reportDate = value))
      .text("Report date in 'yyyy-MM-dd' format")
      .validate(value =>
        reportDateMatcher.findFirstIn(value) match {
          case None => failure(s"Match error in '$value'. Option --report-date expects a date in 'yyyy-MM-dd' format")
          case _    => success
        }
      )

    opt[String]("report-version")
      .required
      .action((value, config) => config.copy(reportVersion = value))
      .text("Report version")
      .validate(value =>
        if (value.toInt > 0) { success }
        else { failure("Option --report-version must be >0") }
      )

    help("help").text("prints this usage text")
  }
}
