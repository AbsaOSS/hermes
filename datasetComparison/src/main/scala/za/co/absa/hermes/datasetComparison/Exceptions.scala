/*
 * Copyright 2019 ABSA Group Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package za.co.absa.hermes.datasetComparison

class DatasetComparisonException(msg: String,
                                 cause: Throwable = None.orNull) extends Exception(msg, cause)

final case class DatasetsDifferException(refPath: String,
                                         stdPath: String,
                                         outPath: String,
                                         expectedCount: Long,
                                         actualCount: Long,
                                         cause: Throwable = None.orNull)
  extends DatasetComparisonException(
    s"""Expected and actual datasets differ.
       |Reference path: $refPath
       |Actual dataset path: $stdPath
       |Difference written to: $outPath
       |Count Expected( $expectedCount ) vs Actual( $actualCount )""".stripMargin,
    cause)

final case class SchemasDifferException(refPath: String,
                                        stdPath: String,
                                        diffSchema: String,
                                        cause: Throwable = None.orNull)
  extends DatasetComparisonException(
    s"""Expected and actual datasets differ in schemas.
       |Reference path: $refPath
       |Actual dataset path: $stdPath
       |Difference is:
       |$diffSchema""".stripMargin,
    cause)

final case class BadProvidedSchema(refPath: String,
                                   stdPath: String,
                                   diffSchema: String,
                                   cause: Throwable = None.orNull)
  extends DatasetComparisonException(
    s"""Provided schema is not a subset of Expected and Actual dataset's schemas.
       |Reference path: $refPath
       |Actual dataset path: $stdPath
       |Difference is:
       |$diffSchema""".stripMargin,
    cause)

final case class DuplicateRowsInDF(countRef: Long, countNew: Long)
  extends DatasetComparisonException(
    s"""Provided datasets have duplicate rows.
       |Reference Dataset has $countRef duplicates
       |New Dataset has $countNew duplicates""".stripMargin)

final case class MissingArgumentException(message: String, cause: Throwable = None.orNull)
  extends Exception(message, cause)
