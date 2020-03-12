---
layout: docs
title: Usage
version: '0.2.0'
categories:
    - '0.2.0'
---
#### Dataset Comparison

General tool for data set comparison. Can be used a spark job or a library. For the library-like usage check method `execute` and `compare` in `DatasetComparisonJob` class in `za.co.absa.hermes.datasetComparison` package.

Example:

```shell
spark-submit dataset-comparison.jar \
    --ref-format csv \
    --ref-path /path/to/csv \
    --ref-header true \
    --new-format parquet \
    --new-path /path/to/parquet \
    --keys ID \
    --out-path /path/to/results
```

This little example would produce a folder `/path/to/results` which would hold the parquet with differences, if there were any and
a `_METRICS` file with some metrics about the comparison.

##### Properties

Properties used in Dataset Comparison can be overriden as standard java opts. For spark that is:

```shell
...
--conf 'spark.driver.extraJavaOptions=-Ddataset-comparison.errColumn=ALFA'
...
```

Properties in use are:

- `errColumn` is a columns that will be appended in the result parquet file, showing location of the difference found in that row
- `tmpColumn` is temporary column name, that will be created for operational purposes and at the end removed
- `comparisonUniqueId` is a unique key column name. Even if you supply multiple keys, they will be combined into this one column
- `actualPrefix` is a prefix of columns comming from the new data source
- `expectedPrefix` is a prefix of columns comming from the referential data source

Default properties are

```json
dataset-comparison {
  errColumn = "errCol"
  tmpColumn = "tmp"
  comparisonUniqueId = "ComparisonUniqueId"
  actualPrefix = "actual"
  expectedPrefix = "expected"
}
```

##### Parameters

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

#### Info File Comparison

Atum's Info file comparison. Ran as part of the E2E Runner but it can be run as a plain old jar file.

```shell
java -jar info-file-comparison.jar \
    --ref-path /path/to/reference/data/_INFO/file \
    --new-path /path/to/new/data/_INFO/file \
    --out-path /path/to/results
```

For _INFO file placed in local repository use format of path `file://path/to/_INFO/file`.

##### Properties

Default properties in use are:

```json
info-file-comparison {
  atum-models {
    stdVersionKey = "std_enceladus_version"
    confVersionKey = "conform_enceladus_version"
    stdNameKey = "Standartization"
    confNameKey = "Conformance"
    stdAppIdKey = "std_application_id"
    confAppIdKey = "conform_application_id"
  }
  comparison-job {
    bufferSizeDefaultValue = 4096
  }
}
```
_Note!_ these will change soon in #63

#### E2E Runner

E2E Runner is a spark job to test end to end data transformations (standardization and conformance from [Enceladus](https://github.com/AbsaOSS/enceladus) project) and compere the output. This tools requires knowledge of the Enceladus project and is highly dependant on it, which is evident from the run example bellow.

```shell
spark-submit e2e-runner.jar \
    --menas-auth-keytab <path_to_keytab_file> \
    --raw-format <raw-format> \
    --dataset-name <dataset-name> \
    --dataset-version <dataset-version> \
    --report-date <report-date> \
    --report-version <report-version> \
    --keys ID1,ID2
```
E2E Runner requires the same input as an Enceladus spark job + `keys` argument. For more about the `keys` argument refer to the Dataset Comparison above. What is important is the data location and properties. All properties are standard JAVA OPTS and can be set using spark's `--conf=spark.driver.extraJavaOptions=`.

##### Properties
Default properties in use are:

```json
e2e-runner {
  stdPath = "/std/{datasetName}/{datasetVersion}/{reportYear}/{reportMonth}/{reportDay}/{reportVersion}"
  confPath = "/publish/{datasetName}/enceladus_info_date={reportYear}-{reportMonth}-{reportDay}/enceladus_info_version={reportVersion}"
  stdParams = ""
  confParams = ""
  dceJarPath = "$DCE_JAR_PATH"
  dceStdName = "standardization.jar"
  dceConfName = "conformance.jar"
  stdClass = "za.co.absa.enceladus.standardization.StandardizationJob"
  confClass = "za.co.absa.enceladus.conformance.DynamicConformanceJob"
  sparkSubmitOptions = "--num-executors 2 --executor-memory 2G --deploy-mode client"
  sparkSubmitExecutable = "spark-submit"
  sparkConf {
    extraJavaOptions {
      "defaultTimestampTimeZone" = "Africa/Johannesburg"
      "menas.rest.uri" = "http://localhost:8080/menas/api"
      "spline.mongodb.name" = "spline"
      "hdp.version" = "2.7.5"
      "conformance.mappingtable.pattern" = "latest"
    }
  }
}
```

Where: (in usage, all keys should be prefixed with `e2e-runner.`)

| Key | Description |
|:---|:---|
| stdPath | Path on which you can expect the new data to be after standardization |
| confPath | Path on which you can expect the new data to be after conformance |
| stdParams | Extra parameters for standardization |
| confParams | Extra parameters for conformance |
| dceJarPath | Path to a folder of Enceladus JAR files |
| dceStdName | Name of the Standardization JAR file in the dceJarPath |
| dceConfName | Name of the Conformance JAR file in the dceJarPath |
| stdClass | Standardization main class to be executed |
| confClass | Conformance main class to be executed |
| sparkSubmitOptions | Spark submit options for the jobs to run with |
| sparkSubmitExecutable | Path to a spark-submit executable |
| sparkConf | Extra spark conf properties for the jobs. For example will be translated into `--conf=spark.driver.extraJavaOptions=-DdefaultTimestampTimeZone=Africa/Johannesburg` |


