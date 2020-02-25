# Enceladus TestUtils
___
<!-- toc -->

- [Dataset Comparison](#dataset-comparison)
- [Info Comparison](#info-comparison)
- [E2E Runner](#e2e-runner)

<!-- tocstop -->
## Build
```bash
sbt assembly
```

Known to work with: 
- Spark 2.4.4
- Java 1.8.0_191-b12
- Scala 2.11.12
- Hadoop 2.7.5 

## <a name="dataset-comparison" />Dataset Comparison
A Spark job for comparing two data sets. 

### Running
Basic running example
```bash
spark-submit \
/path/to/jar/file \
--format <format of the reference and new data sets> \
--new-path /path/to/new/data/set \
--ref-path /path/to/referential/data/set \
--outPath /path/to/diff/output
--keys key1,key2,key3
```

#### Where
```bash
Datasets Comparison 
Usage: spark-submit [spark options] --class za.co.absa.hermes.datasetComparison.DatasetComparisonJob hermes.jar [options]

  -f, --format <value>     format of the raw data (csv, xml, parquet,fixed-width, etc.).
  --rowTag <value>         use the specific row tag instead of 'ROW' for XML format.
  --delimiter <value>      use the specific delimiter instead of ',' for CSV format.
  --header <value>         use the header option to consider CSV header.
  --new-path <value>       Path to the new dataset, just generated and to be tested.
  --ref-path <value>       Path to supposedly correct data set.
  --outPath <value>        Path to where the `ComparisonJob` will save the differences. 
                               This will efectivly creat a folder in which you will find two 
                               other folders. expected_minus_actual and actual_minus_expected.
                               Both hold parque data sets of differences. (minus as in is 
                               relative complement
  --keys                   If there are know unique keys, they can be specified for better
                               output. Keys should be specified one by one, with , (comma) 
                               between them.
  --help                   prints this usage text.
  
  In case comparison of two different formats use these keys instead of the simple ones (--format, --header, --delimiter, --row-tag):
  
  --ref-format <value>     format of the reference raw data (csv, xml, parquet,fixed-width, etc.).
  --ref-delimiter <value>  use the specific delimiter of reference data instead of ',' for CSV format.
  --ref-header <value>     use the header option to consider CSV header of reference data.
  --ref-rowTag <value>     use the specific row tag of reference data instead of 'ROW' for XML format.
  --new-format <value>     format of the new raw data (csv, xml, parquet,fixed-width, etc.).
  --new-delimiter <value>  use the specific delimiter of new data instead of ',' for CSV format.
  --new-header <value>     use the header option to consider CSV header of new data.
  --new-rowTag <value>     use the specific row tag of new data instead of 'ROW' for XML format.
```

Other configurations are Spark dependant and are out of scope of this README.

##  <a name="info-comparison" />Info File Comparison
Autm's Info file comparison. Ran as part of the E2E Runner. Can be run as a spark job or a plain old jar file.

##  <a name="e2e-runner" />E2E Runner
Runs both Standardization and Conformance on the data provided. After each, a comparison job is run 
to check the results against expected reference data.

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
