---
layout: docs
title: Compontent - Dataset Comparison
version: '0.3'
categories:
    - '0.3'
    - components
redirect_from: /docs/components/dataset-comparison
---

#### Table of contents

- [Table of contents](#table-of-contents)
- [Features](#features)
- [Features for spark-job](#features-for-spark-job)
- [Constraints](#constraints)
- [Concepts](#concepts)
  - [Schema alignment](#schema-alignment)
  - [Provided schema](#provided-schema)

#### Features

- compares two datasets and provides a delta file of sorts together with some metrics and metadata
- if keys are provided, the tool is specific in its output, providing paths to differences and showing the diff data.
- schema can be supplied for selective comparison. This will only compare the fields found in the schema

#### Features for spark-job

- can load any data type/format that Apache Spark is able to load. For some of the formats to be supported, additional libraries might need to be added to the classpath
- input data referential or new (being tested) can have different formats
- format of the diff file can be configured as well (default is `parquet`)
- regardless of fail or pass status of the data comparison, a metrics file called `_METRICS` is wirtten to the destination

#### Constraints

- referential schema must be a subset of the new (being verified) data
- without a provided key, the diff file makes a comparison of a row as a whole. With keys, it compares precise data and shows paths to differences

#### Concepts

##### Schema alignment

Dataset comparison takes the referential data and tries to align the new (being verified) data to the schema of the referential data. This means both sorting the columns and only selecting columns that are present in the referential data.

##### Provided schema

If a schema is provided for a dataset comparison, then _schema alignment_ is done against this provided schema for both referential and new data.
