---
layout: docs
title: Build Process
version: '0.3'
categories:
    - '0.3'
redirect_from: /docs/build-process
---

The whole project is SBT and Scala based.

#### Dependencies

The tools are known to work with:

- Java 8
- Scala 2.11.12
- Spark 2.4.X

_A cross-compilation is in the works_

#### Test

`sbt test`

#### Build

##### Build an executable JAR

So called _Fat JAR_

`sbt assembly`

##### Build a library JAR

`sbt package`