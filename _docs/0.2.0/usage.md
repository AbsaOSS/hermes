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
```
spark-submit dataset-comparison.jar \
    --ref-format csv \
    --ref-path /path/to/csv \
    --ref-header true \
    --new-format parquet \
    --new-path /path/to/parquet \
    --keys ID \
    --outPath /path/to/results
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


#### E2E Runner