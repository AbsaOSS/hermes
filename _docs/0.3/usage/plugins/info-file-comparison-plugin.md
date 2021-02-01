---
layout: docs
title: Plugin - Info File Comparison
version: '0.3'
categories:
    - '0.3'
    - 'usage'
    - 'plugins'
redirect_from: /docs/usage/plugins/info-file-comparison-plugin
---

Info file comparison plugin executes a Hermeses Info File Comparison module

Arguments (`args`) are expected in the same format as described in the [documentation]({{ site.baseurl }}/docs/{{ page.version }}/usage/info-file-comparison#parameters) about running an info file comparison.

Write arguments (`writeArgs`) are for now empty and need to be defined empty. In next release we plan to split them from `args`


Example:

```json
{
    "pluginName" : "InfoFileComparison",
    "name": "TestName",
    "order" : 1,
    "args" : ["ref-format", "csv", "ref-path", "/path/to/csv", "ref-header", "true", "new-format", "parquet", "new-path", "/path/to/parquet", "keys", "ID"],
    "writeArgs": []
}
```

