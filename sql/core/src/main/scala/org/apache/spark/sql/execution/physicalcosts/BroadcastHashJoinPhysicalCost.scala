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

package org.apache.spark.sql.execution.physicalcosts

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.sql.execution.{ReusedChild, SparkPlan}
import org.apache.spark.sql.execution.joins.{BuildLeft, BuildRight, BuildSide}

case class BroadcastCostParameters(a: BigDecimal, b: BigDecimal, c: BigDecimal)

case class BroadcastHashJoinCostParameters(a: BigDecimal, b: BigDecimal, c: BigDecimal)

class BroadcastHashJoinPhysicalCost(
  broadcastCostParameters: BroadcastCostParameters,
  broadcastHashJoinCostParameters: BroadcastHashJoinCostParameters,
  left: SparkPlan,
  right: SparkPlan,
  leftRowCount: Option[BigInt],
  rightRowCount: Option[BigInt],
  buildSide: BuildSide,
  totalFields: Int) extends PhysicalCost with Logging with Serializable {

  @transient
  final val sqlContext = SparkSession.getActiveSession.map(_.sqlContext).orNull

  protected def sparkContext = sqlContext.sparkContext

  private def broadcastCost: BigDecimal = {
    val rowCount = buildSide match {
      case BuildLeft => leftRowCount
      case BuildRight => rightRowCount
    }
    if (rowCount.isDefined) {
      val numOfExecutors = sparkContext.getExecutorMemoryStatus.size
      val rowSize = UnsafeRow.calculateFixedPortionByteSize(totalFields)
      val totalSize = rowCount.get * rowSize
      val sizePerExecutor = BigDecimal(totalSize / numOfExecutors)
      broadcastCostParameters.a *
        BigDecimal(Math.pow(sizePerExecutor.toDouble, broadcastCostParameters.b.toDouble)) +
        broadcastCostParameters.c
    } else {
      BigDecimal(0)
    }
  }

  private def joinCost: BigDecimal = {
    val numOfExecutors = sparkContext.getExecutorMemoryStatus.size
    val tasksPerCpu = sparkContext.conf.getInt("spark.task.cpus", 1)
    lazy val coresPerExecutor = sparkContext.conf.getInt("spark.executor.cores", 1)
    val parallelization = math.max(numOfExecutors * (coresPerExecutor / tasksPerCpu), 1)
    val leftParallel = BigDecimal(leftRowCount.getOrElse(BigInt(1)) / parallelization)
    val rightParallel = BigDecimal(rightRowCount.getOrElse(BigInt(1)) / parallelization)
    broadcastHashJoinCostParameters.a + broadcastHashJoinCostParameters.b * leftParallel +
      broadcastHashJoinCostParameters.c * rightParallel.pow(2)
  }

  override lazy val get: BigDecimal = {
    buildSide match {
      case BuildLeft =>
        left match {
          case ReusedChild(_) =>
            joinCost
          case _ =>
            broadcastCost + joinCost
        }
      case BuildRight =>
        right match {
          case ReusedChild(_) =>
            joinCost
          case _ =>
            broadcastCost + joinCost
        }
    }
  }
}
