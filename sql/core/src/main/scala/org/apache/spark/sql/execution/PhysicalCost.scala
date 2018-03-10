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
  cpuCost: BigDecimal,
  ioReadCost: BigDecimal,
  ioWriteCost: BigDecimal,
  netCost: BigDecimal) extends Serializable {
  lazy val get: BigDecimal = {
    val sqlContext = SparkSession.getActiveSession.map(_.sqlContext).orNull
    assert(sqlContext != null)
    val sqlConf = sqlContext.conf

    val cpuWeightedCost = sqlConf.physicalCboCPUWeight * cpuCost
    val ioReadWeightedCost = sqlConf.physicalCboIOReadWeight * ioReadCost
    val ioWriteWeightedCost = sqlConf.physicalCboIOWriteWeight * ioWriteCost
    val netWeightedCost = sqlConf.physicalCboNetWeight * netCost
    cpuWeightedCost + ioReadWeightedCost + ioWriteWeightedCost + netWeightedCost
  }
}
