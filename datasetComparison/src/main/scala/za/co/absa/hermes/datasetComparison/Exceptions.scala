package za.co.absa.hermes.datasetComparison

import org.apache.spark.sql.types.StructField

final case class DatasetsDifferException(refPath: String,
                                         stdPath: String,
                                         outPath: String,
                                         expectedCount: Long,
                                         actualCount: Long,
                                         cause: Throwable = None.orNull)
  extends Exception("Expected and actual datasets differ.\n" +
    s"Reference path: $refPath\n" +
    s"Actual dataset path: $stdPath\n" +
    s"Difference written to: $outPath\n" +
    s"Count Expected( $expectedCount ) vs Actual( $actualCount )", cause)

final case class SchemasDifferException(refPath: String,
                                        stdPath: String,
                                        diffSchema: Seq[StructField],
                                        cause: Throwable = None.orNull)
  extends Exception("Expected and actual datasets differ in schemas.\n" +
    s"Reference path: $refPath\n" +
    s"Actual dataset path: $stdPath\n" +
    s"Difference is $diffSchema", cause)


final case class DuplicateRowsInDF(path: String)
  extends Exception(s"Provided dataset has duplicate rows. Specific rows written to $path")
