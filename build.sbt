ThisBuild / organization := "za.co.absa"
ThisBuild / name         := "hermes"
ThisBuild / version      := "0.1.0-SNAPSHOT"
ThisBuild / scalaVersion := "2.11.12"

val baseDependencies = List(
  "com.github.scopt" %% "scopt"     % "4.0.0-RC2",
  "com.outr"         %% "scribe"    % "2.7.3",
  "com.typesafe"     %  "config"    % "1.3.4",
//  "io.netty" % "netty-all" % "4.1.17.Final",
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

//assemblyMergeStrategy in assembly := {
//  case PathList("org","aopalliance", xs @ _*) => MergeStrategy.last
//  case PathList("javax", "inject", xs @ _*) => MergeStrategy.last
//  case PathList("javax", "servlet", xs @ _*) => MergeStrategy.last
//  case PathList("javax", "activation", xs @ _*) => MergeStrategy.last
//  case PathList("org", "apache", xs @ _*) => MergeStrategy.last
//  case PathList("com", "google", xs @ _*) => MergeStrategy.last
//  case PathList("com", "esotericsoftware", xs @ _*) => MergeStrategy.last
//  case PathList("com", "codahale", xs @ _*) => MergeStrategy.last
//  case PathList("com", "yammer", xs @ _*) => MergeStrategy.last
//  case "about.html" => MergeStrategy.rename
//  case "META-INF/ECLIPSEF.RSA" => MergeStrategy.last
//  case "META-INF/mailcap" => MergeStrategy.last
//  case "META-INF/mimetypes.default" => MergeStrategy.last
//  case "plugin.properties" => MergeStrategy.last
//  case "log4j.properties" => MergeStrategy.last
//  case x =>
//    val oldStrategy = (assemblyMergeStrategy in assembly).value
//    oldStrategy(x)
//}

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