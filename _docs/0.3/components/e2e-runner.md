---
layout: docs
title: Component - E2E Runner
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
- [JSON prescription features](#json-prescription-features)
- [Constraints](#constraints)

#### Features

- run all the tests in order as specified in the JSON 
- extend LoggingFunctions for your own logs implementation

#### Features for spark-job

- load all plugins on the class path 
- load JSON prescription of tests
- fail fast or let all tests execute switch

#### JSON prescription features

- order based 1st on the number and then alphabet
- there can be variables so you do not have to repeat yourself
- tests can depend on each other

#### Constraints

- plugins need to extend trait `za.co.absa.hermes.e2eRunner.Plugin`
- plugins need to return an extension of a trait `za.co.absa.hermes.e2eRunner.PluginResult`
