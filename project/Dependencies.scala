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

import sbt._

object Dependencies {

  private val betterFilesVersion = "3.8.0"
  private val classUtilsVersion = "1.5.0"
  private val hadoopVersion = "2.7.7"
  private val nettyAllVersion = "4.1.36.Final"
  private val reflectionsVersion = "0.9.12"
  private val scoptVersion = "4.0.0"
  private val scribeVersion = "2.7.3"
  private val sparkXmlVersion = "0.5.0"
  private val sprayJsonVersion = "1.3.5"
  private val typeSafeConfigVersion = "1.3.4"

  private val atumModelVersion = "3.5.0"
  private val commonsVersion = "0.0.14"
  private val hofsVersion = "0.4.0"
  private val absaCommonsVersion = "0.0.27"

  private val scalatestVersion = "3.0.5"

  def sparkVersion: String = sys.props.getOrElse("SPARK_VERSION", "2.4.7")

  val baseDependencies = List(
    "com.github.scopt"   %% "scopt"       % scoptVersion,
    "com.outr"           %% "scribe"      % scribeVersion,
    "com.typesafe"       %  "config"      % typeSafeConfigVersion,
    "io.spray"           %%  "spray-json" % sprayJsonVersion,
    "org.scalatest"      %% "scalatest"   % scalatestVersion       % Test,
    "za.co.absa.commons" %% "commons"     % absaCommonsVersion
  )

  val e2eDependencies = List(
    "org.apache.spark"     %% "spark-core"   % sparkVersion     % Provided,
    "org.apache.spark"     %% "spark-sql"    % sparkVersion     % Provided,
    "org.reflections" %  "reflections" % reflectionsVersion,
    "org.clapper"     %% "classutil"   % classUtilsVersion
  )

  val datasetComparisonDependencies = List(
    "org.apache.spark"   %% "spark-core"    % sparkVersion     % Provided,
    "org.apache.spark"   %% "spark-sql"     % sparkVersion     % Provided,
    "org.apache.spark"   %% "spark-avro"    % sparkVersion,
    "com.databricks"     %% "spark-xml"     % sparkXmlVersion,
    "org.apache.hadoop"  %  "hadoop-hdfs"   % hadoopVersion    % Provided,
    "org.apache.hadoop"  %  "hadoop-client" % hadoopVersion    % Provided,
    "io.netty"           %  "netty-all"     % nettyAllVersion,
    "za.co.absa"         %% "spark-hofs"    % hofsVersion,
    "za.co.absa.commons" %% "commons"       % commonsVersion
  )

  val compareInfoFileDependencies = List(
    "org.apache.spark"     %% "spark-core"   % sparkVersion     % Provided,
    "org.apache.spark"     %% "spark-sql"    % sparkVersion     % Provided,
    "za.co.absa"           %% "atum-model"   % atumModelVersion,
    "com.github.pathikrit" %% "better-files" % betterFilesVersion
  )

  val utilsDependencies = List(
    "org.apache.spark" %% "spark-core" % sparkVersion % Provided,
    "org.apache.spark" %% "spark-sql"  % sparkVersion % Provided,
    "com.databricks"   %% "spark-xml"  % sparkXmlVersion
  )
}
