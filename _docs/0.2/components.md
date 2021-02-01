---
layout: docs
title: Components
version: '0.2'
categories:
    - '0.2'
---
#### Table of contents

- [Table of contents](#table-of-contents)
- [Dataset Comparison](#dataset-comparison)
- [Info File Comparison](#info-file-comparison)
- [E2E Runner](#e2e-runner)

Each of the components released by *Hermes* is a command line tool. All are `.jar` files.

#### Dataset Comparison

General tool for data set comparison. Can be used a spark job or a library. For the library-like usage check method `execute` and `compare` in `DatasetComparisonJob` class in `za.co.absa.hermes.datasetComparison` package.

If used as a library you just provide 2 dataframes for comparison. But features in case of usage as a spark job are:

- can load any data type/format that spark is able to load. It might be needed to add libraries to the class path
- input data referential or new (being tested) can have different formats
- if keys are provided, the tool is specific in its output, providing paths to differences and showing the diff data.
- regardless of fail or pass of the data, a metrics file called _METRICS is wirtten to the destination

***

#### Info File Comparison

A simple comparison tool for [Atum][gh-atum]'s _INFO file. Work on local FS and Hadoop. Shipped as `JAR` file

***

#### E2E Runner

E2E Runner is a tool that runs a "full" end to end tests for [Enceladus][gh-enceladus] project and this module was created specificaly for this project. It runs it's spark-jobs and then modules previously mentioned. This is opensourced just to follow the strategy of our organization and to show the possibilities of usage.

[gh-atum]: https://github.com/AbsaOSS/atum
[gh-enceladus]: https://github.com/AbsaOSS/enceladus
