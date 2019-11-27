ThisBuild / organization := "za.co.absa"
ThisBuild / name         := "hermes"
ThisBuild / version      := "0.1.0-SNAPSHOT"
ThisBuild / scalaVersion := "2.11.12"

import Dependencies._

val mergeStrategy: Def.SettingsDefinition = assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case "application.conf" => MergeStrategy.concat
  case x => MergeStrategy.first
}

lazy val datasetComparison = project
  .dependsOn(utils)
  .settings(
    libraryDependencies ++= baseDependencies,
    libraryDependencies ++= datasetComparisonDependencies,
    mainClass in assembly := Some("za.co.absa.hermes.datasetComparison.datasetComparisonJob"),
    test in assembly := {},
    mergeStrategy
  )

lazy val e2eRunner = project
  .dependsOn(datasetComparison, infoFileComparison)
  .settings(
    mainClass in assembly := Some("za.co.absa.hermes.e2eRunner.E2ERunnerJob"),
    test in assembly := {},
    mergeStrategy
  )

lazy val infoFileComparison = project
  .settings(
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
