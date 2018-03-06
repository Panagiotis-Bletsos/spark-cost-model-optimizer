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

import java.nio.charset.StandardCharsets
import java.sql.{Date, Timestamp}

import scala.collection.mutable

import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{AnalysisException, Row, SparkSession}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.UnsupportedOperationChecker
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, ReturnAnswer}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.execution.command.{DescribeTableCommand, ExecutedCommandExec, ShowTablesCommand}
import org.apache.spark.sql.execution.exchange.{EnsureRequirements, ReuseExchange}
import org.apache.spark.sql.execution.joins.{BroadcastHashJoinExec, PlanLaterJoin, SortMergeJoinExec}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{BinaryType, DateType, DecimalType, TimestampType, _}
import org.apache.spark.util.Utils

/**
 * The primary workflow for executing relational queries using Spark.  Designed to allow easy
 * access to the intermediate phases of query execution for developers.
 *
 * While this is not a public class, we should avoid changing the function names for the sake of
 * changing them, because a lot of developers use the feature for debugging.
 */
class QueryExecution(val sparkSession: SparkSession, val logical: LogicalPlan) extends Logging {

  // TODO: Move the planner an optimizer into here from SessionState.
  protected def planner = sparkSession.sessionState.planner

  def assertAnalyzed(): Unit = {
    // Analyzer is invoked outside the try block to avoid calling it again from within the
    // catch block below.
    analyzed
    try {
      sparkSession.sessionState.analyzer.checkAnalysis(analyzed)
    } catch {
      case e: AnalysisException =>
        val ae = new AnalysisException(e.message, e.line, e.startPosition, Option(analyzed))
        ae.setStackTrace(e.getStackTrace)
        throw ae
    }
  }

  def assertSupported(): Unit = {
    if (sparkSession.sessionState.conf.isUnsupportedOperationCheckEnabled) {
      UnsupportedOperationChecker.checkForBatch(analyzed)
    }
  }

  lazy val analyzed: LogicalPlan = {
    SparkSession.setActiveSession(sparkSession)
    sparkSession.sessionState.analyzer.execute(logical)
  }

  lazy val withCachedData: LogicalPlan = {
    assertAnalyzed()
    assertSupported()
    sparkSession.sharedState.cacheManager.useCachedData(analyzed)
  }

  lazy val optimizedPlan: LogicalPlan = sparkSession.sessionState.optimizer.execute(withCachedData)

  lazy val sparkPlan: SparkPlan = {
    SparkSession.setActiveSession(sparkSession)
    // TODO: We use next(), i.e. take the first plan returned by the planner, here for now,
    //       but we will implement to choose the best plan.
    planner.plan(ReturnAnswer(optimizedPlan)).next()
  }

  lazy val optimalSparkPlan: SparkPlan = {
    val startTime = System.nanoTime()
    val plans = CostBasedQueryPlanner(sparkPlan, sparkSession.sessionState.conf)
    val executedPlans = plans map prepareForExecution
    val bestPlan = executedPlans.minBy(_.map(_.cost.get).sum)
    val durationInMs = (System.nanoTime() - startTime) / (1000 * 1000)
    logError(s"Optimal plan found. Duration: $durationInMs ms")
    bestPlan
  }

  // executedPlan should not be used to initialize any SparkPlan. It should be
  // only used for execution.
  lazy val executedPlan: SparkPlan = {
    if (sparkSession.sqlContext.conf.physicalCboOptimizerEnabled) {
      optimalSparkPlan
    } else {
      prepareForExecution(sparkPlan)
    }
  }

  /** Internal version of the RDD. Avoids copies and has no schema */
  lazy val toRdd: RDD[InternalRow] = executedPlan.execute()

  /**
   * Prepares a planned [[SparkPlan]] for execution by inserting shuffle operations and internal
   * row format conversions as needed.
   */
  protected def prepareForExecution(plan: SparkPlan): SparkPlan = {
    preparations.foldLeft(plan) { case (sp, rule) => rule.apply(sp) }
  }

  /** A sequence of rules that will be applied in order to the physical plan before execution. */
  protected def preparations: Seq[Rule[SparkPlan]] = Seq(
    python.ExtractPythonUDFs,
    PlanSubqueries(sparkSession),
    EnsureRequirements(sparkSession.sessionState.conf),
    CollapseCodegenStages(sparkSession.sessionState.conf),
    ReuseExchange(sparkSession.sessionState.conf),
    ReuseSubquery(sparkSession.sessionState.conf))

  protected def stringOrError[A](f: => A): String =
    try f.toString catch { case e: AnalysisException => e.toString }


  /**
   * Returns the result as a hive compatible sequence of strings. This is for testing only.
   */
  def hiveResultString(): Seq[String] = executedPlan match {
    case ExecutedCommandExec(desc: DescribeTableCommand) =>
      // If it is a describe command for a Hive table, we want to have the output format
      // be similar with Hive.
      desc.run(sparkSession).map {
        case Row(name: String, dataType: String, comment) =>
          Seq(name, dataType,
            Option(comment.asInstanceOf[String]).getOrElse(""))
            .map(s => String.format(s"%-20s", s))
            .mkString("\t")
      }
    // SHOW TABLES in Hive only output table names, while ours output database, table name, isTemp.
    case command @ ExecutedCommandExec(s: ShowTablesCommand) if !s.isExtended =>
      command.executeCollect().map(_.getString(1))
    case other =>
      val result: Seq[Seq[Any]] = other.executeCollectPublic().map(_.toSeq).toSeq
      // We need the types so we can output struct field names
      val types = analyzed.output.map(_.dataType)
      // Reformat to match hive tab delimited output.
      result.map(_.zip(types).map(toHiveString)).map(_.mkString("\t"))
  }

  /** Formats a datum (based on the given data type) and returns the string representation. */
  private def toHiveString(a: (Any, DataType)): String = {
    val primitiveTypes = Seq(StringType, IntegerType, LongType, DoubleType, FloatType,
      BooleanType, ByteType, ShortType, DateType, TimestampType, BinaryType)

    def formatDecimal(d: java.math.BigDecimal): String = {
      if (d.compareTo(java.math.BigDecimal.ZERO) == 0) {
        java.math.BigDecimal.ZERO.toPlainString
      } else {
        d.stripTrailingZeros().toPlainString
      }
    }

    /** Hive outputs fields of structs slightly differently than top level attributes. */
    def toHiveStructString(a: (Any, DataType)): String = a match {
      case (struct: Row, StructType(fields)) =>
        struct.toSeq.zip(fields).map {
          case (v, t) => s""""${t.name}":${toHiveStructString(v, t.dataType)}"""
        }.mkString("{", ",", "}")
      case (seq: Seq[_], ArrayType(typ, _)) =>
        seq.map(v => (v, typ)).map(toHiveStructString).mkString("[", ",", "]")
      case (map: Map[_, _], MapType(kType, vType, _)) =>
        map.map {
          case (key, value) =>
            toHiveStructString((key, kType)) + ":" + toHiveStructString((value, vType))
        }.toSeq.sorted.mkString("{", ",", "}")
      case (null, _) => "null"
      case (s: String, StringType) => "\"" + s + "\""
      case (decimal, DecimalType()) => decimal.toString
      case (other, tpe) if primitiveTypes contains tpe => other.toString
    }

    a match {
      case (struct: Row, StructType(fields)) =>
        struct.toSeq.zip(fields).map {
          case (v, t) => s""""${t.name}":${toHiveStructString(v, t.dataType)}"""
        }.mkString("{", ",", "}")
      case (seq: Seq[_], ArrayType(typ, _)) =>
        seq.map(v => (v, typ)).map(toHiveStructString).mkString("[", ",", "]")
      case (map: Map[_, _], MapType(kType, vType, _)) =>
        map.map {
          case (key, value) =>
            toHiveStructString((key, kType)) + ":" + toHiveStructString((value, vType))
        }.toSeq.sorted.mkString("{", ",", "}")
      case (null, _) => "NULL"
      case (d: Date, DateType) =>
        DateTimeUtils.dateToString(DateTimeUtils.fromJavaDate(d))
      case (t: Timestamp, TimestampType) =>
        DateTimeUtils.timestampToString(DateTimeUtils.fromJavaTimestamp(t),
          DateTimeUtils.getTimeZone(sparkSession.sessionState.conf.sessionLocalTimeZone))
      case (bin: Array[Byte], BinaryType) => new String(bin, StandardCharsets.UTF_8)
      case (decimal: java.math.BigDecimal, DecimalType()) => formatDecimal(decimal)
      case (other, tpe) if primitiveTypes.contains(tpe) => other.toString
    }
  }

  def simpleString: String = {
    s"""== Physical Plan ==
       |${stringOrError(executedPlan.treeString(verbose = false))}
      """.stripMargin.trim
  }

  override def toString: String = completeString(appendStats = false)

  def toStringWithStats: String = completeString(appendStats = true)

  private def completeString(appendStats: Boolean): String = {
    def output = Utils.truncatedString(
      analyzed.output.map(o => s"${o.name}: ${o.dataType.simpleString}"), ", ")
    val analyzedPlan = Seq(
      stringOrError(output),
      stringOrError(analyzed.treeString(verbose = true))
    ).filter(_.nonEmpty).mkString("\n")

    val optimizedPlanString = if (appendStats) {
      // trigger to compute stats for logical plans
      optimizedPlan.stats(sparkSession.sessionState.conf)
      optimizedPlan.treeString(verbose = true, addSuffix = true)
    } else {
      optimizedPlan.treeString(verbose = true)
    }

    s"""== Parsed Logical Plan ==
       |${stringOrError(logical.treeString(verbose = true))}
       |== Analyzed Logical Plan ==
       |$analyzedPlan
       |== Optimized Logical Plan ==
       |${stringOrError(optimizedPlanString)}
       |== Physical Plan ==
       |${stringOrError(executedPlan.treeString(verbose = true))}
    """.stripMargin.trim
  }

  /** A special namespace for commands that can be used to debug query execution. */
  // scalastyle:off
  object debug {
  // scalastyle:on

    /**
     * Prints to stdout all the generated code found in this plan (i.e. the output of each
     * WholeStageCodegen subtree).
     */
    def codegen(): Unit = {
      // scalastyle:off println
      println(org.apache.spark.sql.execution.debug.codegenString(executedPlan))
      // scalastyle:on println
    }
  }
}

object CostBasedQueryPlanner extends Logging {
  private case class PhysicalJoinCandidate(
    broadcastHashJoinExec: BroadcastHashJoinExec,
    sortMergeJoinExec: SortMergeJoinExec) {
    def cross(other: PhysicalJoinCandidate): Seq[Seq[SparkPlan]] = {
      for { x <- this.toSeq; y <- other.toSeq } yield Seq(x, y)
    }

    def cross(other: Seq[Seq[SparkPlan]]): Seq[Seq[SparkPlan]] = {
      if (other.nonEmpty) {
        val c: Seq[Seq[Any]] = for { x <- this.toSeq; y <- other } yield Seq(x, y)
        c.map(_.flatMap {
          case s: Seq[SparkPlan] => s.flatMap(Seq(_))
          case sp: SparkPlan => Seq(sp)
        })
      } else {
        Seq(Seq(this.broadcastHashJoinExec), Seq(this.sortMergeJoinExec))
      }
    }

    private def toSeq: Seq[SparkPlan] = Seq(broadcastHashJoinExec, sortMergeJoinExec)
  }

  private def extractPhysicalJoinCandidates(sparkPlan: SparkPlan): Seq[PhysicalJoinCandidate] = {
    sparkPlan match {
      case plj @ PlanLaterJoin(leftKeys, rightKeys, joinType, buildSide, condition, left, right,
      leftRowCount, rightRowCount, rowCount) =>
        extractPhysicalJoinCandidates(left) ++ extractPhysicalJoinCandidates(right) ++ Seq(
          PhysicalJoinCandidate(
            BroadcastHashJoinExec(leftKeys, rightKeys, joinType, buildSide, condition, left, right),
            SortMergeJoinExec(leftKeys, rightKeys, joinType, condition, left, right)))
      case uen: UnaryExecNode =>
        extractPhysicalJoinCandidates(uen.child) ++ Seq.empty
      case ben: BinaryExecNode =>
        extractPhysicalJoinCandidates(ben.left) ++ extractPhysicalJoinCandidates(ben.right) ++ Nil
      case len: LeafExecNode => Nil
      case sp: SparkPlan => sp.children.flatMap(extractPhysicalJoinCandidates) ++ Nil
      case _ => Nil
    }
  }

  private def extractCombinations(
    res: Seq[Seq[SparkPlan]],
    physicalJoinCandidates: Seq[PhysicalJoinCandidate],
    maxDepth: Int,
    depth: Int): Seq[Seq[SparkPlan]] = {
    if (maxDepth == depth + 1 || physicalJoinCandidates.size == 1) {
      physicalJoinCandidates.head cross res
    } else {
      physicalJoinCandidates.head cross extractCombinations(
        res, physicalJoinCandidates.drop(1), maxDepth, depth + 1)
    }
  }

  def apply(sparkPlan: SparkPlan, sqlConf: SQLConf): Seq[SparkPlan] = {
    val startTime = System.nanoTime()

    val physicalJoinCandidates = extractPhysicalJoinCandidates(sparkPlan)
    if (physicalJoinCandidates.isEmpty) {
      return Seq(sparkPlan)
    }

    val maxDepthToCheck = {
      if (physicalJoinCandidates.size > sqlConf.physicalCboOptimizerMaxDepth) {
        sqlConf.physicalCboOptimizerMaxDepth
      } else {
        physicalJoinCandidates.size
      }
    }
    val combinations: Seq[Seq[SparkPlan]] = extractCombinations(
      Seq.empty[Seq[SparkPlan]], physicalJoinCandidates, maxDepthToCheck, 0)
    val newPlans = Seq.fill[SparkPlan](combinations.size)(sparkPlan)

    val plans = newPlans.zipWithIndex.map {
      case (newPlan: SparkPlan, index: Int) =>
        var depth = 0
        val combination = combinations(index)
        val joins = mutable.HashMap[(StructType, Option[Expression]), SparkPlan]()
        newPlan transformUp {
          case plj @ PlanLaterJoin(leftKeys, rightKeys, joinType, buildSide, condition, left, right,
          leftRowCount, rightRowCount, rowCount) if depth < maxDepthToCheck =>
            val currentDepth = depth
            depth += 1
            joins.put((plj.schema, condition), combination(currentDepth))
            combination(currentDepth) match {
              case bhj: BroadcastHashJoinExec => BroadcastHashJoinExec(
                leftKeys, rightKeys, joinType, buildSide, condition, left, right, rowCount)
              case smj: SortMergeJoinExec =>
                SortMergeJoinExec(leftKeys, rightKeys, joinType, condition, left, right,
                  leftRowCount, rightRowCount, rowCount)
              case _ =>
                logWarning("Unknown join algorithm")
                BroadcastHashJoinExec(
                  leftKeys, rightKeys, joinType, buildSide, condition, left, right, rowCount)
            }
          case plj @ PlanLaterJoin(
          leftKeys, rightKeys, joinType, buildSide, condition, left, right, leftRowCount,
          rightRowCount, rowCount) =>
            val sameJoin = joins.get((plj.schema, condition))
            if (sameJoin.isDefined) {
              sameJoin match {
                case Some(BroadcastHashJoinExec(_, _, _, _, _, _, _, _, _, _)) =>
                  BroadcastHashJoinExec(leftKeys, rightKeys, joinType, buildSide, condition, left,
                    right, leftRowCount, rightRowCount)
                case Some(SortMergeJoinExec(_, _, _, _, _, _, _, _, _)) =>
                  SortMergeJoinExec(leftKeys, rightKeys, joinType, condition, left, right,
                    leftRowCount, rightRowCount)
              }
            } else {
              BroadcastHashJoinExec(leftKeys, rightKeys, joinType, buildSide, condition, left,
                right, leftRowCount, rightRowCount)
            }
        }
    }
    val durationInMs = (System.nanoTime() - startTime) / (1000 * 1000)
    logInfo(s"New query planner finished. Duration: $durationInMs ms, number of plans: " +
      s"${plans.length}, number of total candidates ${physicalJoinCandidates.size}")
    if (plans.nonEmpty) plans else Seq(sparkPlan)
  }
}
