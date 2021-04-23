# Enceladus TestUtils

- [Enceladus TestUtils](#enceladus-testutils)
  - [To Build](#to-build)
  - [Dataset Comparison](#dataset-comparison)
    - [Running](#running)
      - [Where](#where)
  - [Info File Comparison](#info-file-comparison)
    - [Running](#running-1)
  - [E2E Runner](#e2e-runner)

Hermes is an E2E testing tool created mainly for the use in [ABSA OSS][gh-absa] ecosystem but still provides some tools/utils that are usable in other projects and are quite generic. For more information, please look at our [Hermes Github Pages][gh-pages].

## To Build

Use either of the commands below. Depending on your versions.
```bash
sbt ++2.11.12 assembly -DSPARK_VERSION=2.4.7
sbt ++2.12.12 assembly -DSPARK_VERSION=2.4.7
sbt ++2.12.12 assembly -DSPARK_VERSION=3.1.1
```

Known to work with:

- Spark 2.4 and 3.1
- Java 1.8
- Scala 2.11.12 and 2.12.12

## Dataset Comparison

Spark job for the comparison of data sets. As it leverages spark, there are almost no limitations to data sources and size of the data.

### Running

Basic running example
```bash
spark-submit \
/path/to/jar/file \
--format <format of the reference and new data sets> \
--new-path /path/to/new/data/set \
--ref-path /path/to/referential/data/set \
--out-path /path/to/diff/output
--keys key1,key2,key3
```

#### Where
```bash
Datasets Comparison 
Usage: spark-submit [spark options] --class za.co.absa.hermes.datasetComparison.DatasetComparisonJob hermes.jar [options]

  --[ref|new]-format            Format of the raw data (csv, xml, parquet,fixed-width, etc.). Use prefix only in case
                                    comparison of two different formats. Mandatory.
  --new-path|--new-dbtable      Path to the new dataset or dbtable (jdbc), just generated and to be tested. Mandatory.
  --ref-path|--ref-dbtable      Path to supposedly correct data set or dbtable (jdbc). Mandatory.
  --out-path.                    Path to where the `ComparisonJob` will save the differences. 
                                    This will efectivly creat a folder in which you will find two 
                                    other folders. expected_minus_actual and actual_minus_expected.
                                    Both hold parque data sets of differences. (minus as in is 
                                    relative complement. Mandatory.
  --keys                        If there are know unique keys, they can be specified for better
                                   output. Keys should be specified one by one, with , (comma) 
                                   between them. Optional.
  others                        Other options depends on selected format specifications (e.g. --delimiter and --header for
                                   csv, --rowTag for xml). For case comparison of two different formats use prefix ref|new
                                   for each of this options. For more information, check sparks documentation on what all
                                   the options for the format you are using. Optional.
  
  --help                   prints similar text to this one.
  
```

Other configurations are Spark dependant and are out of scope of this README.

## Info File Comparison

[Autm][atum]'s (and it's derivatives) Info file comparison. Ran as part of the E2E Runner. Can be run as a plain old jar file.

### Running

Basic running example

```bash
java -jar \
/path/to/jar/file \
--new-path /path/to/new/data/set \
--ref-path /path/to/referential/data/set \
--out-path /path/to/diff/output
```

## E2E Runner

Currently runs both Standardization and Conformance of [Enceladus][enceladus] project on the data provided. After each, a comparison job is run to check the results against expected reference data.

This tool is planed for an upgrade in the nearest future to be a general E2E Runner for user defined runes.

Basic running example:

```bash
spark-submit \
/path/to/jar/file \
--menas-credentials-file /path/to/credentials/file \
--dataset-name <datasetName> \
--dataset-version <datasetVersion> \
--report-date <reportData> \
--report-version <reportVersion> \
--raw-format <rawFormat>
--keys <key1,key2,...>
```

[gh-absa]: https://github.com/AbsaOSS
[gh-pages]: https://absaoss.github.io/hermes/
[atum]: https://github.com/AbsaOSS/atum