---
layout: docs
title: Plugin - Dataset Comparison
version: '0.3'
categories:
    - '0.3'
    - 'usage'
    - 'plugins'
redirect_from: /docs/usage/plugins/dataset-comparison-plugin
---

Dataset comparison plugin executes a Hermes's Dataset Comparison module

Arguments (`args`) are expected in the same format as described in the [documentation]({{ site.baseurl }}/docs/{{ page.version }}/usage/dataset-comparison#parameters) about running a dataset comparison spark job. Only excluding `out-path`.

Arguments (`writeArgs`) are to affect the output and are provided in the same way as above. But only some are valid like `out-path`, `format`, etc. These are mandatory.

Example:

```json
{
    "pluginName" : "DatasetComparison",
    "name": "TestName",
    "order" : 1,
    "args" : ["ref-format", "csv", "ref-path", "/path/to/csv", "ref-header", "true", "new-format", "parquet", "new-path", "/path/to/parquet", "keys", "ID"],
    "writeArgs": ["out-path", "/path/to/result"]
}
```

