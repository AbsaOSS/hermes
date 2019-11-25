ThisBuild / organization := "za.co.absa"
ThisBuild / name         := "hermes"
ThisBuild / version      := "0.1.0-SNAPSHOT"
ThisBuild / scalaVersion := "2.11.12"

val baseDependencies = List(
  "com.github.scopt" %% "scopt"     % "4.0.0-RC2",
  "com.outr"         %% "scribe"    % "2.7.3",
  "com.typesafe"     %  "config"    % "1.3.4",
  "org.scalatest"    %% "scalatest" % "3.0.5"      % Test
)

val datasetComparisonDependencies = List(
  "org.apache.spark" %% "spark-core" % "2.4.4" % Provided,
  "org.apache.spark" %% "spark-sql" % "2.4.4" % Provided,
  "com.databricks" %% "spark-xml" % "0.5.0",
  "org.apache.hadoop" % "hadoop-hdfs" % "2.7.7" % Provided,
  "org.apache.hadoop" % "hadoop-client" % "2.7.7" % Provided,
  "io.netty" % "netty-all" % "4.1.36.Final"
)

val compareInfoFileDependencies = List(
  "za.co.absa" % "atum" % "0.2.5",
  "com.github.pathikrit" %% "better-files" % "3.8.0"

)

val utilsDependencies = List(
  "org.apache.spark" %% "spark-core" % "2.4.4" % Provided,
  "org.apache.spark" %% "spark-sql" % "2.4.4" % Provided,
  "com.databricks" %% "spark-xml" % "0.5.0"
)

lazy val datasetComparison = project
  .dependsOn(utils)
  .settings(
    libraryDependencies ++= baseDependencies,
    libraryDependencies ++= datasetComparisonDependencies,
    test in assembly := {},
    assemblyMergeStrategy in assembly := {
      case PathList("META-INF", xs @ _*) => MergeStrategy.discard
      case x => MergeStrategy.first
    }
  )

lazy val e2eRunner = project
  .dependsOn(datasetComparison, infoFileComparison)
  .settings(
    mainClass in assembly := Some("za.co.absa.hermes.e2eRunner.E2ERunnerJob"),
    test in assembly := {},
    assemblyMergeStrategy in assembly := {
      case PathList("META-INF", xs @ _*) => MergeStrategy.discard
      case "application.conf" => MergeStrategy.concat
      case x => MergeStrategy.first
    }
  )

lazy val infoFileComparison = project
  .settings(
    libraryDependencies ++= baseDependencies,
    libraryDependencies ++= compareInfoFileDependencies,
    test in assembly := {},
    assemblyMergeStrategy in assembly := {
      case PathList("META-INF", xs @ _*) => MergeStrategy.discard
      case x => MergeStrategy.first
    }
  )

lazy val utils = project
  .settings(
    libraryDependencies ++= baseDependencies,
    libraryDependencies ++= utilsDependencies,
    test in assembly := {},
    assemblyMergeStrategy in assembly := {
      case PathList("META-INF", xs @ _*) => MergeStrategy.discard
      case x => MergeStrategy.first
    }
  )
