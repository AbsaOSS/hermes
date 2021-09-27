/*
 * Copyright 2019 ABSA Group Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package za.co.absa.hermes.datasetComparison

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import za.co.absa.hermes.datasetComparison.dataFrame.S3Location.StringS3LocationExt

import java.net.URI

case class PathResolver(fs: FileSystem, fullPath: String, basePath: String) {
  def fsExists: Boolean = fs.exists(new Path(basePath))

  def updatePathSuffix(newBasePath: String): PathResolver = {
    val newFullPath = fullPath.replaceAllLiterally(basePath, newBasePath)
    PathResolver(fs, newFullPath, newBasePath)
  }

  def getPathWithTimestamp: PathResolver = {
    updatePathSuffix(s"${basePath}_${System.currentTimeMillis .toString}")
  }
}

object PathResolver {
  /**
   * Converts string full path to Hadoop FS and Path, e.g.
   * `s3://mybucket1/path/to/file` -> S3 FS + `path/to/file`
   * `/path/on/hdfs/to/file` -> local HDFS + `/path/on/hdfs/to/file`
   *
   * Note, that non-local HDFS paths are not supported in this method, e.g. hdfs://nameservice123:8020/path/on/hdfs/too.
   *
   * @param pathString path to convert to FS and relative path
   * @return FS + relative path
   **/
  def pathStringToFsWithPath(pathString: String, conf: Configuration): PathResolver = {
    pathString.toS3Location match {
      case Some(s3Location) =>
        val s3Uri = new URI(s3Location.s3String) // s3://<bucket>
        val s3Path = s"/${s3Location.path}" // /<text-file-object-path>

        val fs = FileSystem.get(s3Uri, conf)
        PathResolver(fs, s3Uri.toString, s3Path)

      case None => // local hdfs location
        val fs = FileSystem.get(conf)
        PathResolver(fs, pathString, pathString)
    }
  }
}
