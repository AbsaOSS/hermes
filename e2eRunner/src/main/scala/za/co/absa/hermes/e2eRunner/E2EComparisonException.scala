package za.co.absa.hermes.e2eRunner

final case class E2EComparisonException(msg: String)
  extends Exception(s"Some comparison(s) failed\n$msg")
