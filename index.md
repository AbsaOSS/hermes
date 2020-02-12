---
layout: default
title: Hermes
---

### What is hermes

Hermes is an E2E testing tool created mainly for the use in [ABSA OSS](https://github.com/AbsaOSS) ecosystem but still provides some tools/utils that are usable in other projects and are quite generic.

#### Hermes is divided into 4 modules:
- **Utils** where user can find some general utilities for file manipulation or spark test base. These should in future be all moved in [ABSA OSS Commons](https://github.com/AbsaOSS/commons)
- **Dataset comparison** is a spark job for the comparison of data sets. As it leverages spark, there are almost no limitations to data sources.
- **Info File comparison** is a CLI tool (with ability to connect to HDFS) for comparison of _INFO files produced by [Atum](https://github.com/AbsaOSS/atum)
- **E2E Runner** is a tool to easily run Standardization and Conformance from [Enceladus](https://github.com/AbsaOSS/enceladus) and then compare the outputs to expected ones


### Future
Hermes is an ongoing project. For now only new module thought about is REST API client and test tool. But we are open to suggestions

### Contributions
Contributions are always welcome in any form. For more, please read our [Contribution guide]({{ site.baseurl }}/contribute).