/**
 * SparklineData, Inc. -- http://www.sparklinedata.com/
 *
 * Scala based Audience Behavior APIs
 *
 * Copyright 2014-2015 SparklineData, Inc
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
 /**
 * Created by Harish.
 */
 package org.sparkline.utils

import java.io.File

object PartitionUtils {

  val defaultPartitionName = "__HIVE_DEFAULT_PARTITION__"

  def getPartitionDir(
                        basePath: File,
                        partitionCols: (String, Any)*): File = {
    val partNames = partitionCols.map { case (k, v) =>
      val valueString = if (v == null || v == "") defaultPartitionName else v.toString
      s"$k=$valueString"
    }

    val partDir = partNames.foldLeft(basePath) { (parent, child) =>
      new File(parent, child)
    }

    partDir
  }

  def makePartitionDir(
                                  basePath: File,
                                  partitionCols: (String, Any)*): File = {
    val partDir = getPartitionDir(basePath, partitionCols:_*)
    assert(partDir.mkdirs(), s"Couldn't create directory $partDir")
    partDir
  }

}
