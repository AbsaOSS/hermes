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

lazy val scala211 = "2.11.12"
lazy val scala212 = "2.12.12"

ThisBuild / scalaVersion := scala211
ThisBuild / crossScalaVersions := Seq(scala211, scala212)
ThisBuild / releaseCrossBuild := true

Global / concurrentRestrictions += Tags.limit(Tags.Test, 1)

import Dependencies._
import sbtrelease.ReleasePlugin.autoImport.ReleaseTransformations._
import com.github.sbt.jacoco.report.JacocoReportSettings

val mergeStrategy: Def.SettingsDefinition = assemblyMergeStrategy in assembly := {
  case PathList("META-INF", _) => MergeStrategy.discard
  case "application.conf"      => MergeStrategy.concat
  case "reference.conf"        => MergeStrategy.concat
  case _                       => MergeStrategy.first
}

releaseProcess := Seq[ReleaseStep](
  checkSnapshotDependencies,
  inquireVersions,
  runClean,
  runTest,
  setReleaseVersion,
  commitReleaseVersion,
  tagRelease,
  releaseStepCommand("publishSigned"),
//  releaseStepCommand("sonatypeBundleRelease"),
  setNextVersion,
  commitNextVersion,
  pushChanges
)

lazy val commonJacocoReportSettings: JacocoReportSettings = JacocoReportSettings(
  formats = Seq(JacocoReportFormats.HTML, JacocoReportFormats.XML)
)

lazy val commonJacocoExcludes: Seq[String] = Seq(
//  "za.co.absa.hermes.utils.SparkCompatibility*", // class and related objects
//  "za.co.absa.hermes.utils.FileReader" // class only
)

lazy val hermes = (project in file("."))
  .settings(
    name := "hermes",
    releaseVersionBump := sbtrelease.Version.Bump.Minor,
    // No need to publish the aggregation [empty] artifact
    publishArtifact := false,
    publish := {},
    publishLocal := {}
  )
  .aggregate(datasetComparison, e2eRunner, infoFileComparison, utils)

lazy val datasetComparison = project
  .dependsOn(utils)
  .settings(
    name := "dataset-comparison",
    libraryDependencies ++= baseDependencies,
    libraryDependencies ++= datasetComparisonDependencies,
    mainClass in assembly := Some("za.co.absa.hermes.datasetComparison.DatasetComparisonJob"),
    test in assembly := {},
    mergeStrategy,
    artifact in (Compile, assembly) := {
      val art = (artifact in (Compile, assembly)).value
      art.withClassifier(Some("assembly"))
    },
    addArtifact(artifact in (Compile, assembly), assembly)
  )
  .settings(
    jacocoReportSettings := commonJacocoReportSettings.withTitle(s"hermes:dataset-comparison_scala_${scalaVersion.value} Jacoco Report"),
    jacocoExcludes := commonJacocoExcludes
  )
  .enablePlugins(AutomateHeaderPlugin)

lazy val e2eRunner = project
  .dependsOn(datasetComparison, infoFileComparison)
  .settings(
    libraryDependencies ++= baseDependencies,
    libraryDependencies ++= e2eDependencies,
    name := "e2e-runner",
    mainClass in assembly := Some("za.co.absa.hermes.e2eRunner.E2ERunnerJob"),
    test in assembly := {},
    mergeStrategy,
    artifact in (Compile, assembly) := {
      val art = (artifact in (Compile, assembly)).value
      art.withClassifier(Some("assembly"))
    },
    addArtifact(artifact in (Compile, assembly), assembly)
  )
  .settings(
    jacocoReportSettings := commonJacocoReportSettings.withTitle(s"hermes:e2e-runner_scala_${scalaVersion.value} Jacoco Report"),
    jacocoExcludes := commonJacocoExcludes
  )
  .enablePlugins(AutomateHeaderPlugin)

lazy val infoFileComparison = project
  .dependsOn(utils)
  .settings(
    name := "info-file-comparison",
    libraryDependencies ++= baseDependencies,
    libraryDependencies ++= compareInfoFileDependencies,
    mainClass in assembly := Some("za.co.absa.hermes.infoFileComparison.InfoFileComparisonJob"),
    test in assembly := {},
    mergeStrategy,
    artifact in (Compile, assembly) := {
      val art = (artifact in (Compile, assembly)).value
      art.withClassifier(Some("assembly"))
    },
    addArtifact(artifact in (Compile, assembly), assembly)
  )
  .settings(
    jacocoReportSettings := commonJacocoReportSettings.withTitle(s"hermes:info-file-comparison_scala_${scalaVersion.value} Jacoco Report"),
    jacocoExcludes := commonJacocoExcludes
  )
  .enablePlugins(AutomateHeaderPlugin)

lazy val utils = project
  .settings(
    libraryDependencies ++= baseDependencies,
    libraryDependencies ++= utilsDependencies,
    test in assembly := {},
    mergeStrategy,
    artifact in (Compile, assembly) := {
      val art = (artifact in (Compile, assembly)).value
      art.withClassifier(Some("assembly"))
    },
    addArtifact(artifact in (Compile, assembly), assembly)
  )
  .settings(
    jacocoReportSettings := commonJacocoReportSettings.withTitle(s"hermes:utils_scala_${scalaVersion.value} Jacoco Report"),
    jacocoExcludes := commonJacocoExcludes
  )
  .enablePlugins(AutomateHeaderPlugin)
