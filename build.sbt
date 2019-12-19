/*
 * Copyright 2019 ABSA Group Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

ThisBuild / organization := "za.co.absa.hermes"
ThisBuild / name         := "hermes"
ThisBuild / scalaVersion := "2.11.12"

import Dependencies._

val mergeStrategy: Def.SettingsDefinition = assemblyMergeStrategy in assembly := {
  case PathList("META-INF", _) => MergeStrategy.discard
  case "application.conf"      => MergeStrategy.concat
  case _                       => MergeStrategy.first
}

lazy val hermes = (project in file("."))
  .settings(
    name := "hermes",

    // No need to publish the aggregation [empty] artifact
    publishArtifact := false,
    publish := {},
    publishLocal := {}
  ).aggregate(datasetComparison, e2eRunner, infoFileComparison, utils)

lazy val datasetComparison = project
  .dependsOn(utils)
  .settings(
    name := "dataset-comparison",
    libraryDependencies ++= baseDependencies,
    libraryDependencies ++= datasetComparisonDependencies,
    mainClass in assembly := Some("za.co.absa.hermes.datasetComparison.datasetComparisonJob"),
    test in assembly := {},
    mergeStrategy
  )

lazy val e2eRunner = project
  .dependsOn(datasetComparison, infoFileComparison)
  .settings(
    name := "e2e-runner",
    mainClass in assembly := Some("za.co.absa.hermes.e2eRunner.E2ERunnerJob"),
    test in assembly := {},
    mergeStrategy
  )

lazy val infoFileComparison = project
  .settings(
    name := "info-file-comparison",
    libraryDependencies ++= baseDependencies,
    libraryDependencies ++= compareInfoFileDependencies,
    mainClass in assembly := Some("za.co.absa.hermes.infoFileComparison.InfoFileComparisonJob"),
    test in assembly := {},
    mergeStrategy
  )

lazy val utils = project
  .settings(
    libraryDependencies ++= baseDependencies,
    libraryDependencies ++= utilsDependencies,
    test in assembly := {},
    mergeStrategy
  )
