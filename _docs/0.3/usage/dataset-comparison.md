---
layout: docs
title: Usage - Dataset Comparison
version: '0.3'
categories:
    - '0.3'
    - usage
redirect_from: /docs/usage/dataset-comparison
---

#### Table of contents

- [Table of contents](#table-of-contents)
- [Properties](#properties)
  - [Available properties](#available-properties)
  - [Default properties are](#default-properties-are)
- [Parameters](#parameters)
- [Usage as library](#usage-as-library)

#### Properties

Properties used in Dataset Comparison can be overridden as standard java opts. For Apache Spark that is:

```shell
...
--conf 'spark.driver.extraJavaOptions=-Ddataset-comparison.errColumn=ALFA'
...
```

##### Available properties

- `errColumn` is a column that will be appended in the result parquet file, showing the location of the difference found in that row
- `actualPrefix` is a prefix of columns coming from the new data source
- `expectedPrefix` is a prefix of columns coming from the referential data source
- `allowDuplicates` is a switch to allow or disallow duplicate rows in comparisons; duplicates will be ignored in case of setting to true

##### Default properties are

```json
dataset-comparison {
  errColumn = "errCol"
  actualPrefix = "actual"
  expectedPrefix = "expected"
  allowDuplicates = false
}
```

#### Parameters

Dataset Comparison can receive the following parameters:

- `keys`
- `schema`
- input/output general ones
- input/output specific ones
 
All should be prefixed with `--` as is standard in command line tools to denote parameter/key followed by its value.

- General parameters will be applied to both reference, new and out data. These have no prefix. So `--format` or `--rowTag` is enough. Only exception is `out-format`. It is by default set to `parquet` and needs to be explicitly overwritten as described in the next point.
- Specific parameters differentiate if you want to use them for the reference data, new data or output. They are prefixed either with `ref-`, `new-` or `out-`. They override the general ones. 
- `keys` is a list of strings delimited by a `,` (comma) that represent the primary key(s)
- `schema` is a path to schema used for cherrypicking the dataframes. Will be read using Spark's `HadoopConfiguration`.
- 
Input parameters (be it general or specific) are governed by the spark datasource formats and options. Imagine having a scala spark code:

```scala
sparkSession
    .read
    .format(<format specified>)
    .options(<map of options provided>)
    .load(<path provided>)
```

In the same way, provide the Dataset Comparison spark job with `format`, but prefix it with `--`, `path`, but prefix it if either `--ref-`, `--new-` or `--out`, depending on the origin and direction. Optionally, add a `--header` option if you chose `csv` format or `--rowTag`, if you chose `XML`.

Now how to use it all together:

- `--format`, `--new-format`, `--ref-format` for choosing the format type. This can be `parquet`, `csv`, `xml`, `jdbc`, etc.
- `--ref-path`, `--new-path`, and `--out-path` define paths on HDFS of the reference data path, newly created data path, and diff output path, respectively. When using the `jdbc` format, these can be replaced by `--ref-dbtable` and `--new-dbtable`
- `--keys` to specify the primary key(s) of the table, e.g. `--keys ID1, ID2`. These can be only top-level columns.
- format specific keys prefixed with `--`, so `--header`, `--rowTag`, `--user`, etc.

**Mandatory fields** are the paths/dbtables, format (at least the general one) and any format-specific keys that are prescribed by the format used.

**NOTE** if you want to use the `jdbc` format or others requiring specific drivers, you should make sure that appropriate drivers can be found on the classpath.


#### Usage as library

Add DatasetComparison dependency to your package manager. 

```scala
libraryDependencies += "za.co.absa.hermes" %% "dataset-comparison" % "X.Y.Z"
```

Then import `DatasetComparator`

```scala
import za.co.absa.hermes.datasetComparison.DatasetComparator
```

Finally create new `DatasetComparator` and call compare. Do not forget that an implicit spark session is needed. More details about the class bellow.

```scala
val cr: ComparisonResult  = new DatasetComparator(df1, df2).compare
```

**Prescription of DatasetComparator class**:

```scala
/**
 * Class that is the brain of the DatasetComparison module. This class should be used in case of using DatasetComparison
 * as a library. In case of running the DatasetComparison as SparkJob, please use the DatasetComparisonJob.
 *
 * @param dataFrameReference is the Dataframe used in comparison as the origin of truth
 * @param dataFrameActual is a Dataframe that is being tested
 * @param keys is a set of primary keys of the dataset. This highly increases the accuracy of the output as we are then able
 *             to pinpoint the differences
 * @param config Config object holding project-based configurable parameters. The difference to the cliOptions is that these
 *               are meant to stay the same for the project, while cliOptions change for each test
 * @param optionalSchema Optional schema to cherry-pick columns from the two DataFrames to compare. For example, if you
 *                       have a timestamp column that will never be the same; you provide a schema without that timestamp
 *                       and it will not be compared.
 * @param sparkSession Implicit spark session.
 */
class DatasetComparator(dataFrameReference: DataFrame,
                        dataFrameActual: DataFrame,
                        keys: Set[String] = Set.empty[String],
                        config: DatasetComparisonConfig = new TypesafeConfig(None),
                        optionalSchema: Option[StructType] = None)
                       (implicit sparkSession: SparkSession)
```

Create an instance of `DatasetComparator` and then invoke the parametrless `compare` method. This method returns a `ComparisonResult`.

**Prescription of ComparisonResult case class**

```scala
/**
 *
 * @param refRowCount Row Count of the reference data
 * @param newRowCount Row Count of the new data
 * @param usedSchemaSelector Selector used to align schemas created from reference data schema
 * @param resultDF Result DataFrame, if None, there were no differences between reference and new data
 * @param diffCount Number of differences if there are any
 * @param passedOptions Raw options passed to the job by the user. It might be empty if comparison used as a library
 */
case class ComparisonResult(refRowCount: Long,
                            newRowCount: Long,
                            refDuplicateCount: Long,
                            newDuplicateCount: Long,
                            passedCount: Long,
                            usedSchemaSelector: List[Column],
                            resultDF: Option[DataFrame],
                            diffCount: Long = 0,
                            passedOptions: String = "",
                            additionalInfo: Map[String, String] = Map.empty)
```