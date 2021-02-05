---
layout: docs
title: Components
version: '0.3'
categories:
    - '0.3'
redirect_from: /docs/components
---

#### Table of contents

- [Table of contents](#table-of-contents)
- [Dataset Comparison](#dataset-comparison)
- [Info File Comparison](#info-file-comparison)
- [E2E Runner](#e2e-runner)

Each of the components released by *Hermes* is a command line tool. All are `.jar` files.

#### Dataset Comparison

General tool for data set comparison. Can be used as a spark job or a library. For the library-like usage, use methods `za.co.absa.hermes.datasetComparison.DatasetComparisonJob.{execute, compare}`.

If used as a library just provide 2 dataframes for comparison and optionaly a set of unique keys and a schema.

[More...]({{ site.baseurl }}/docs/{{ page.version }}/components/dataset-comparison)

***

#### Info File Comparison

A simple comparison tool for [Atum][gh-atum]'s _INFO file. It works on local FS and Hadoop FS.

***

#### E2E Runner

E2E Runner is a tool that runs end-to-end tests based on the JSON input file. E2R Runner has a **plugin architecture** so the type of test depends on the plugins loaded. 

Built in Plugins are
- Bash command plugin
- Dataset Comparison plugin
- Info File Comaprison plugin

[More...]({{ site.baseurl }}/docs/{{ page.version }}/components/e2e-runner)

[gh-atum]: https://github.com/AbsaOSS/atum
[gh-enceladus]: https://github.com/AbsaOSS/enceladus
