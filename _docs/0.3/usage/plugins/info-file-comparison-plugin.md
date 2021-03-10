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

Info file comparison plugin executes a Hermes's Info File Comparison module

Arguments (`args`) are expected in the same format as described in the [documentation]({{ site.baseurl }}/docs/{{ page.version }}/usage/info-file-comparison#parameters) about running an info file comparison.

Write arguments (`writeArgs`) have to be empty now. In future releases, it is planed to split them from `args`


Example:

```json
{
    "pluginName" : "InfoFileComparison",
    "name": "TestName",
    "order" : 1,
    "args" : ["--new-path", "/input2/_INFO", "--ref-path", "/input2/_INFO", "--out-path", "/stdInfoDiff"],
    "writeArgs": []
}
```
