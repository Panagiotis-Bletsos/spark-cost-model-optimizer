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

package org.apache.spark.sql.execution.benchmark

import java.io.{File, FileOutputStream, PrintStream, PrintWriter}

import org.apache.commons.io.output.TeeOutputStream

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation
import org.apache.spark.sql.catalyst.expressions.SubqueryExpression
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.util._
import org.apache.spark.util.Benchmark

/**
 * Benchmark to measure TPCDS query performance.
 * To run this:
 *  spark-submit --class <this class> --jars <spark sql test jar>
 */
object TPCDSQueryBenchmark extends Logging {
  val conf =
    new SparkConf()
      .setMaster("local[4]")
      .setAppName("test-sql-context")
      .set("spark.sql.parquet.compression.codec", "snappy")
      .set("spark.sql.shuffle.partitions", "4")
      .set("spark.driver.memory", "3g")
      .set("spark.executor.memory", "3g")
      .set("spark.sql.autoBroadcastJoinThreshold", (20 * 1024 * 1024).toString)
      .set("spark.sql.crossJoin.enabled", "true")
      .set("spark.sql.cbo.enabled", "true")
      .set("spark.sql.cbo.joinReorder.enabled", "true")

  val spark = SparkSession.builder.config(conf).enableHiveSupport().getOrCreate()

  spark.sparkContext.setLogLevel("INFO")

  val tables = Seq("catalog_page", "catalog_returns", "customer", "customer_address",
    "customer_demographics", "date_dim", "household_demographics", "inventory", "item",
    "promotion", "store", "store_returns", "catalog_sales", "web_sales", "store_sales",
    "web_returns", "web_site", "reason", "call_center", "warehouse", "ship_mode", "income_band",
    "time_dim", "web_page")

  def setupTables(dataLocation: String): Map[String, Long] = {
    tables.map { tableName =>
//      spark.read.parquet(s"$dataLocation/$tableName").createOrReplaceTempView(tableName)
      if (tableName == "inventory" || tableName == "store_returns") {
        spark.read.parquet(s"$dataLocation/$tableName").createOrReplaceTempView(tableName)
        spark.sql(s"CREATE TABLE IF NOT EXISTS $tableName USING HIVE OPTIONS" +
          s"(fileFormat 'parquet') AS SELECT * FROM $tableName")
      }
      spark.sql(s"CREATE TABLE IF NOT EXISTS $tableName USING HIVE OPTIONS" +
        s"(fileFormat 'parquet') AS SELECT * FROM $tableName")
      tableName -> spark.table(tableName).count()
    }.toMap
  }

  def tpcdsAll(dataLocation: String, queries: Seq[String]): Unit = {
    require(dataLocation.nonEmpty,
      "please modify the value of dataLocation to point to your local TPCDS data")
    val tableSizes = setupTables(dataLocation)
    val out = new FileOutputStream(s"analyze-commands.txt")
    val printStream = new PrintStream(new TeeOutputStream(System.out, out))
    tables foreach { tableName =>
      val columns = spark.sql(s"SELECT * FROM $tableName").columns
//      // scalastyle:off println
//      printStream.println(s"ANALYZE TABLE $tableName COMPUTE STATISTICS FOR COLUMNS " +
//        s"${columns.mkString(", ")}\n")
//      // scalastyle:on println
      spark.sql(s"ANALYZE TABLE $tableName COMPUTE STATISTICS FOR COLUMNS ${columns.mkString(", " +
        "")}")
    }
    queries.foreach { name =>
      logInfo(name)

      val queryString = fileToString(new File
      (s"sql/core/target/scala-2.11/test-classes/tpcds/$name.sql"))

      // This is an indirect hack to estimate the size of each query's input by traversing the
      // logical plan and adding up the sizes of all tables that appear in the plan. Note that this
      // currently doesn't take WITH subqueries into account which might lead to fairly inaccurate
      // per-row processing time for those cases.
      val queryRelations = scala.collection.mutable.HashSet[String]()
      spark.sql(queryString).queryExecution.logical.map {
        case ur @ UnresolvedRelation(t: TableIdentifier) =>
          queryRelations.add(t.table)
        case lp: LogicalPlan =>
          lp.expressions.foreach { _ foreach {
            case subquery: SubqueryExpression =>
              subquery.plan.foreach {
                case ur @ UnresolvedRelation(t: TableIdentifier) =>
                  queryRelations.add(t.table)
                case _ =>
              }
            case _ =>
          }
        }
        case _ =>
      }
      val numRows = queryRelations.map(tableSizes.getOrElse(_, 0L)).sum
      val benchmark = new Benchmark(s"TPCDS Snappy", numRows, 1, output = Option(
        new FileOutputStream(s"results-$name.txt")))
      val out = new FileOutputStream(s"explain-cost.txt")
      val printStream = new PrintStream(new TeeOutputStream(System.out, out))
      benchmark.addCase(name) { i =>
        // scalastyle:off println
        printStream.println(spark.sql(s"EXPLAIN COST $queryString").collect().map(_.mkString)
          .mkString)
        // scalastyle:on println

        spark.sql(queryString).collect()
      }
      benchmark.run()
    }
  }

  def main(args: Array[String]): Unit = {

    // List of all TPC-DS queries
//    val tpcdsQueries = Seq(
//      "q1", "q2", "q3", "q4", "q5", "q6", "q7", "q8", "q9", "q10", "q11",
//      "q12", "q13", "q14a", "q14b", "q15", "q16", "q17", "q18", "q19", "q20",
//      "q21", "q22", "q23a", "q23b", "q24a", "q24b", "q25", "q26", "q27", "q28", "q29", "q30",
//      "q31", "q32", "q33", "q34", "q35", "q36", "q37", "q38", "q39a", "q39b", "q40",
//      "q41", "q42", "q43", "q44", "q45", "q46", "q47", "q48", "q49", "q50",
//      "q51", "q52", "q53", "q54", "q55", "q56", "q57", "q58", "q59", "q60",
//      "q61", "q62", "q63", "q64", "q65", "q66", "q67", "q68", "q69", "q70",
//      "q71", "q72", "q73", "q74", "q75", "q76", "q77", "q78", "q79", "q80",
//      "q81", "q82", "q83", "q84", "q85", "q86", "q87", "q88", "q89", "q90",
//      "q91", "q92", "q93", "q94", "q95", "q96", "q97", "q98", "q99")

    val tpcdsQueries = Seq("q25")
    // In order to run this benchmark, please follow the instructions at
    // https://github.com/databricks/spark-sql-perf/blob/master/README.md to generate the TPCDS data
    // locally (preferably with a scale factor of 5 for benchmarking). Thereafter, the value of
    // dataLocation below needs to be set to the location where the generated data is stored.
    val dataLocation = "/Users/nospa/thesis/tpcds-parquet"

    tpcdsAll(dataLocation, queries = tpcdsQueries)
  }
}
