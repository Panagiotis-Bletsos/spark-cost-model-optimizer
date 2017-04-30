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

package gr.ionio.p11blet

import org.apache.logging.log4j.scala.Logging

import org.apache.spark.SparkContext
import org.apache.spark.sql._
import org.apache.spark.sql.execution.debug._

object TestCostModel extends Logging {

  def main(args : Array[String]): Unit = {
    if (args.isEmpty) throw new IllegalArgumentException("Must be at least 1 argument " +
      "representing an SQL clause")
    if (args(0) == null) throw new IllegalArgumentException("First argument must be an SQL clause")

    new SparkContext().setLogLevel("DEBUG")

    val sparkSession = SparkSession
      .builder()
      .appName("Spark SQL Simple Example")
      .master("local[*]")
      .config("spark.logConf", true)
      // .config("spark.shuffle.spill.numElementsForceSpillThreshold", 9000)
      .getOrCreate()

    def isInDebugMode(arg: String) : Boolean = arg match {
      case "true" => true
      case "false" => false
      case _ => throw new IllegalArgumentException("Second argument must be `true` or `false`")
    }

    val debug = if (args.length == 2) isInDebugMode(args(1)) else false

    driver(sparkSession, args(0), debug)
  }

  private def driver(sparkSession: SparkSession,
                     sqlClause: String,
                     debug: Boolean) : Unit = sqlClause match {
    case "select" => runSelectQuery(sparkSession, debug)
    case "join" => runJoinQuery(sparkSession, debug)
    case _ => throw new IllegalArgumentException(s"Unknown clause: $sqlClause")
  }

  private def runSelectQuery(sparkSession: SparkSession, debug: Boolean) : Unit = {
    val item = sparkSession.read
      .jdbc(Database.db, "item", Database.connectionProperties)

    item.createOrReplaceTempView("item")

    val sqlQuery = sparkSession.sql("SELECT * FROM item")

    selectQueryMode(sqlQuery, debug)
  }

  private def runJoinQuery(sparkSession: SparkSession, debug: Boolean) : Unit = {
    val item = sparkSession.read
      .jdbc(Database.db, "item", Database.connectionProperties)
    val promotion = sparkSession.read
      .jdbc(Database.db, "promotion", Database.connectionProperties)

    item.createOrReplaceTempView("item")
    promotion.createOrReplaceTempView("promotion")

    val sqlQuery = sparkSession.sql("SELECT * FROM ITEM INNER JOIN promotion ON i_item_sk = " +
      "p_item_sk")

    selectQueryMode(sqlQuery, debug)
  }

  private def selectQueryMode(query: DataFrame, debug: Boolean) = {
    if (debug) query.debugCodegen() else query.show()
  }

}
