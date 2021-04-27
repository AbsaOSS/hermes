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
- Scala 2.11.12 or 2.12.12
- Spark 2.4.2 to 3.1.X

#### Test

Depending on your versions of scala or spark use one of:

`sbt ++2.11.12 -DSPARK_VERSION=2.4.7 test`
`sbt ++2.12.12 -DSPARK_VERSION=2.4.7 test`
`sbt ++2.12.12 -DSPARK_VERSION=3.1.1 test`

#### Build

##### Build an executable JAR

For so called _Fat JAR_, depending on your versions of scala or spark, use one of:

`sbt ++2.11.12 -DSPARK_VERSION=2.4.7 assembly`
`sbt ++2.12.12 -DSPARK_VERSION=2.4.7 assembly`
`sbt ++2.12.12 -DSPARK_VERSION=3.1.1 assembly`

##### Build a library JAR

Depending on your versions of scala or spark use one of:

`sbt ++2.11.12 -DSPARK_VERSION=2.4.7 package`
`sbt ++2.12.12 -DSPARK_VERSION=2.4.7 package`
`sbt ++2.12.12 -DSPARK_VERSION=3.1.1 package`
