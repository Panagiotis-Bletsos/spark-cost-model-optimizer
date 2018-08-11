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

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.sql.execution.{ReusedChild, SparkPlan}

class SortMergeJoinPhysicalCost(
  exchangeWeight: Double,
  sortWeight: Double,
  joinWeight: Double,
  left: SparkPlan,
  right: SparkPlan,
  leftRowCount: Option[BigInt],
  rightRowCount: Option[BigInt],
  leftTotalFields: Int,
  rightTotalFields: Int) extends PhysicalCost with Serializable {

  @transient
  final val sqlContext = SparkSession.getActiveSession.map(_.sqlContext).orNull

  protected def sparkContext = sqlContext.sparkContext

  private def joinCost: BigDecimal = {
    val numOfExecutors = sparkContext.getExecutorMemoryStatus.size
    val tasksPerCpu = sparkContext.conf.getInt("spark.task.cpus", 1)
    lazy val coresPerExecutor = sparkContext.conf.getInt("spark.executor.cores", 1)
    val parallelization = math.max(numOfExecutors * (coresPerExecutor / tasksPerCpu), 1)
    val processingRowsInParallel = BigDecimal(
      leftRowCount.getOrElse(BigInt(1)) * rightRowCount.getOrElse(BigInt(1)) / parallelization)
    joinWeight * processingRowsInParallel
  }

  private def sortCost(rowCount: Option[BigInt]): BigDecimal = {
    if (rowCount.isDefined) {
      val numOfExecutors = sparkContext.getExecutorMemoryStatus.size
      val tasksPerCpu = sparkContext.conf.getInt("spark.task.cpus", 1)
      val coresPerExecutor = sparkContext.conf.getInt("spark.executor.cores", 1)
      val parallelization = math.max(numOfExecutors * (coresPerExecutor / tasksPerCpu), 1)
      val processingRowsInParallel = (rowCount.get / parallelization).toDouble
      sortWeight * processingRowsInParallel
    } else {
      BigDecimal(0)
    }
  }

  private def exchangeCost(rowCount: Option[BigInt], totalFields: Int): BigDecimal = {
    if (rowCount.isDefined) {
      val numOfExecutors = sparkContext.getExecutorMemoryStatus.size
      val tasksPerCpu = sparkContext.conf.getInt("spark.task.cpus", 1)
      val coresPerExecutor = sparkContext.conf.getInt("spark.executor.cores", 1)
      val parallelization = math.max(numOfExecutors * (coresPerExecutor / tasksPerCpu), 1)
      val processingRowsInParallel = BigDecimal(rowCount.get / parallelization)
      val rowSize = UnsafeRow.calculateFixedPortionByteSize(totalFields)
      exchangeWeight * processingRowsInParallel * rowSize
    } else {
      BigDecimal(0)
    }
  }

  private def exchangeLeftCost = {
    left match {
      case ReusedChild(_) => BigDecimal(0)
      case _ => exchangeCost(leftRowCount, leftTotalFields)
    }
  }

  private def exchangeRightCost = {
    right match {
      case ReusedChild(_) => BigDecimal(0)
      case _ => exchangeCost(rightRowCount, rightTotalFields)
    }
  }

  override lazy val get: BigDecimal = {
    exchangeLeftCost + exchangeRightCost + sortCost(leftRowCount) + sortCost(rightRowCount) +
      joinCost
  }
}
