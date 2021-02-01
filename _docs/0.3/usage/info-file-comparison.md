---
layout: docs
title: Usage - Info File Comparison
version: '0.3'
categories:
    - '0.3'
    - usage
redirect_from: /docs/usage/info-file-comparison
---

##### Parameters

- `ref-path` - mandatory - Path to the referential _INFO file
- `new-path` - mandatory - Path to the new (to be tested) _INFO file
- `out-path` - mandatory - Path to the diff file

##### Properties

Default properties in use are:

```json
info-file-comparison {
  atum-models {
    versionMetaKeys = [
      "std_enceladus_version",
      "conform_enceladus_version"
    ]
    ignoredMetaKeys = [
      "std_application_id",
      "conform_application_id",
      "conform_input_dir_size",
      "std_input_dir_size",
      "std_output_dir_size",
      "conform_output_dir_size"
    ]
  }
  comparison-job {
    bufferSizeDefaultValue = 4096
  }
}
```

| Key | Description |
|:---|:---|
| versionMetaKeys | Keys from additional data in info file that you want to log as versions. These are not going to be compared by the tool, but both versions will be printed out |
| ignoredMetaKeys | Keys from additional data in info file that you want to completely ignore. These are going to be logged out |
| bufferSizeDefaultValue | This is Hadoop configuration's `io.file.buffer.size` for writing the results to Hadoop |