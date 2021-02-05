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
  - [Avaivable Properties](#avaivable-properties)
  - [Default properties are](#default-properties-are)
- [Parameters](#parameters)
- [Usage as library](#usage-as-library)

#### Properties

Properties used in Dataset Comparison can be overriden as standard java opts. For spark that is:

```shell
...
--conf 'spark.driver.extraJavaOptions=-Ddataset-comparison.errColumn=ALFA'
...
```

##### Avaivable Properties

- `errColumn` is the name of a column that will be appended in the result parquet file, showing location of the difference found in that row
- `actualPrefix` is a prefix for columns coming from the new data source
- `expectedPrefix` is a prefix of columns comming from the referential data source
- `allowDuplicates` is a switch to allow or disallow duplicate rows in comparisons; duplicates will be ignored

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

Dataset Comparison can recieve two types of parameters + `keys` and `out-path`. There are input general, input specific, `keys` and `out-path`. All should be prefixed with `--` as is standard in command line tools to denote parameter/key followed by its value.

- General parameters are those that will be applied to both reference and new data. These have no prefix
- Specific parameters are those that differentiate if you want to use them for reference or new data source. They are prefixed eaither with `ref-` or `new-`. They override the general ones
- `keys` is a list of strings delimetered by a `,` (comma) that represent the primary key(s)
- `out-path` is a path to a directory, that will be created by the job, and will hold the diff dataframe and `_METRICS` file

Input parameters (be it general or specific) are governed by the spark datasource formats and options. Imagine having a scala spark code:

```scala
sparkSession
    .read
    .format(<format specified>)
    .options(<map of options provided>)
    .load(<path provided>)
```

Same way you would provide the Dataset Comparison spark job with `format`, but prefix it with `--`, `path`, but prefix it if either `--ref-` or `--new-`, dependeing on the origing. Maybe add a `--header` option, if you chose `CSV` format or `--rowTag`, if you chose `XML`.

Now how to use it all together:

- `--format`, `--new-format`, `--ref-format` for choosing the format type. This can be `parquet`, `csv`, `xml`, `jdbc`, etc.
- `--ref-path`, `--new-path` and `--out-path` define paths on HDFS of reference data path, newly created data path and diff output path, repspectively. In case of usage of `jdbc` format these can be replaced by `--ref-dbtable` and `--ned-dbtable`
- `--keys` to specifie the primary key(s) of the table, e.g. `--keys ID1, ID2`. These can be only top level columns.
- format specific keys prefixed with `--`, so `--header`, `--rowTag`, `--user`, etc.

**Mandatory fields** are logically the paths/dbtables, format (at least the general one) and any format specific keys that are prescribed by the format used.

**NOTE** if you want to use `jdbc` format or others, requiring specific drivers, you should have appropriate drivers on a class path.


#### Usage as library

For usage as library use class `za.co.absa.hermes.datasetComparison.DatasetComparator`. 

**Prescription of DatasetComparator class**:

```scala
/**
 * Class that is the brain of the DatasetComparison module. This class should be used in case of using DatasetComparison
 * as a library. In case of running the DatasetComparison as SparkJob, please use the DatasetComparisonJob.
 *
 * @param dataFrameReference is a Dataframe used in a comparison as the origin of truth
 * @param dataFrameActual is a Dataframe that is being tested
 * @param keys is a set of primary keys of the dataset. This highly increases accuracy of the output as we are then able
 *             to pinpoint the differences
 * @param config Config object holding project based configurable parameters. Difference to the cliOptions is that these
 *               are meant to stay the same for the project, while cliOptions change for each test
 * @param optionalSchema Optional schema to cherry-pick columns form the two dataframes to compare. For example, if you
 *                       have a timestamp column that will never be the same, you provide a schema without that timestamp
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

You create an instance of `DatasetComparator` and then invoke parametrless `compare` method. This method returns `ComparisonResult`.

**Prescription of ComparisonResult case class**

```scala
/**
 *
 * @param refRowCount Row Count of the reference data
 * @param newRowCount Row Count of the new data
 * @param usedSchemaSelector Selector used to align schemas created from reference data schema
 * @param resultDF Result dataframe, if None, there were no differences between reference and new data
 * @param diffCount Number of differences if there are any
 * @param passedOptions Raw options passed to the job by user. Might be empty if comparison used as a library
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
