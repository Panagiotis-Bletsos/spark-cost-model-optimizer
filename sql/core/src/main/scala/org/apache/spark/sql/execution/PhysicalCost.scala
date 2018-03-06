/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.execution

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.internal.SQLConf

class PhysicalCost(
  cpuCost: Double, ioReadCost: Double, ioWriteCost: Double, netCost: Double) extends Serializable {
  lazy val get: Double = {
    assert(cpuCost > 0 && ioReadCost > 0 && ioWriteCost > 0 && netCost > 0)

    val sqlContext = SparkSession.getActiveSession.map(_.sqlContext).orNull
    assert(sqlContext != null)
    val sqlConf = sqlContext.conf

    val cpuWeightedCost = sqlConf.physicalCboCPUWeight * Math.log(cpuCost)
    val ioReadWeightedCost = sqlConf.physicalCboIOReadWeight * Math.log(ioReadCost)
    val ioWriteWeightedCost = sqlConf.physicalCboIOWriteWeight * Math.log(ioWriteCost)
    val netWeightedCost = sqlConf.physicalCboNetWeight * Math.log(netCost)
    cpuWeightedCost + ioReadWeightedCost + ioWriteWeightedCost + netWeightedCost
  }
}
