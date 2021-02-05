---
layout: docs
title: Usage
version: '0.3'
categories:
    - '0.3'
redirect_from: /docs/usage
---
#### Table of contents

- [Table of contents](#table-of-contents)
- [Dataset Comparison](#dataset-comparison)
- [Info File Comparison](#info-file-comparison)
- [E2E Runner](#e2e-runner)
- [Plugins](#plugins)

***

#### Dataset Comparison

Dataset comparison can be run as a spark job or used as a library.

Dataset Comparison used as a spark job example. _This example doesn't show spark arguments_
Example:

```shell
spark-submit dataset-comparison.jar \
    --ref-format csv \
    --ref-path /path/to/csv-dir \ // will pickup `data.csv` in the directory
    --ref-header true \
    --new-format parquet \
    --new-path /path/to/parquet \
    --keys ID \
    --out-path /path/to/results
```

This little example would produce a folder `/path/to/results` which would hold the parquet with differences, if there were any and
a `_METRICS` file with some metrics about the comparison.

[More...]({{ site.baseurl }}/docs/{{ page.version }}/usage/dataset-comparison)

***

#### Info File Comparison

Atum's Info file comparison. Ran as part of the E2E Runner but it can be run as a plain old jar file.

```shell
java -jar info-file-comparison.jar \
    --ref-path /path/to/reference/data/_INFO \
    --new-path /path/to/new/data/_INFO \
    --out-path /path/to/results
```

For _INFO file placed in local repository use format of path `file://path/to/_INFO`.

[More...]({{ site.baseurl }}/docs/{{ page.version }}/usage/info-file-comparison)

***

#### E2E Runner

E2E usage shifted the most since 0.2.2. Now it can be used to run any test that there is a plugin for. It can be run as a spark job or a classic jar. On input, it expects a maximum of 3 arguments: _path to test definitions_, _jar-path to additional plugins_, and _fail-fast switch_.

```shell
spark-submit e2e-runner.jar \
    --test-definition-path /some/path/testDefinition.json \
    --jar-path /extra/jar/folder \
    --fail-fast true
```

[More...]({{ site.baseurl }}/docs/{{ page.version }}/usage/e2e-runner)

#### Plugins

There are 3 built-in plugins. These are all out of the box usable with the E2E Runner. 

- [BashPlugin]({{ site.baseurl }}/docs/{{ page.version }}/usage/plugins/bash-plugin)
- [DatasetComparisonPlugin]({{ site.baseurl }}/docs/{{ page.version }}/usage/plugins/dataset-comparison-plugin)
- [InfoFileComparisonPlugin]({{ site.baseurl }}/docs/{{ page.version }}/usage/plugins/info-file-comparison-plugin)

Then there is an option of creating a plugin tailored for specific need(s) following this [guide]({{ site.baseurl }}/docs/{{ page.version }}/usage/plugins/plugin-creation)

[gh-enceladus]: https://github.com/AbsaOSS/enceladus
