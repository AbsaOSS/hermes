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
  val baseDependencies = List(
    "com.github.scopt" %% "scopt"     % "4.0.0-RC2",
    "com.outr"         %% "scribe"    % "2.7.3",
    "com.typesafe"     %  "config"    % "1.3.4",
    "io.spray"         %%  "spray-json" % "1.3.5",
    "org.scalatest"    %% "scalatest" % "3.0.5"      % Test
  )

  val e2eDependencies = List(
    "org.reflections"    % "reflections"   % "0.9.12",
    "org.clapper"       %% "classutil"   % "1.5.0"
  )

  val datasetComparisonDependencies = List(
    "org.apache.spark"   %% "spark-core"           % "2.4.6"        % Provided,
    "org.apache.spark"   %% "spark-sql"            % "2.4.6"        % Provided,
    "com.databricks"     %% "spark-xml"            % "0.5.0",
    "org.apache.hadoop"  %  "hadoop-hdfs"          % "2.7.7"        % Provided,
    "org.apache.hadoop"  %  "hadoop-client"        % "2.7.7"        % Provided,
    "io.netty"           %  "netty-all"            % "4.1.36.Final",
    "org.postgresql"     %  "postgresql"           % "42.2.9"       % Provided,
    "org.xerial"         %   "sqlite-jdbc"         % "3.30.1"       % Provided,
    "mysql"              %  "mysql-connector-java" % "8.0.19"       % Provided,
    "com.oracle.ojdbc"   %  "ojdbc8"               % "19.3.0.0"     % Provided,
    "org.apache.hive"    %  "hive-jdbc"            % "1.2.1"        % Provided,
    "org.apache.spark"   %% "spark-avro"           % "2.4.4",
    "za.co.absa"         %% "spark-hofs"           % "0.4.0",
    "za.co.absa.commons" %% "commons"              % "0.0.14"
  )

  val compareInfoFileDependencies = List(
    "za.co.absa"           %  "atum"         % "0.2.6",
    "com.github.pathikrit" %% "better-files" % "3.8.0"
  )

  val utilsDependencies = List(
    "org.apache.spark" %% "spark-core" % "2.4.4" % Provided,
    "org.apache.spark" %% "spark-sql"  % "2.4.4" % Provided,
    "com.databricks"   %% "spark-xml"  % "0.5.0"
  )
}
