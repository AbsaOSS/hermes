package za.co.absa.hermes.datasetComparison

case class CliHelpOptions(key: String, optional: String, text: String){
  override def toString: String = f"$key%-26s$optional%-11s$text"
}

case class CliHelp(title: String, example: String, description: String, options: List[CliHelpOptions]) {
  override def toString: String =
    s"""$title
       |$description
       |$example
       |Options:
       |${options.mkString("\n")}
       |""".stripMargin
}
