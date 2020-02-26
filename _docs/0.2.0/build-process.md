---
layout: docs
title: Build Process
version: '0.2.0'
categories:
    - '0.2.0'
---

The whole project is sbt and scala based.

#### Dependencies

The projects is known to work with:
- Java 1.8
- Scala 2.11
- Hadoop 2.7.X
- Spark 2.4.X

#### Test

```scala
sbt test
```

#### Build

```scala
sbt assembly
```