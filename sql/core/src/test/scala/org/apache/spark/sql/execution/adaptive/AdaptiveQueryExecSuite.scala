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

package org.apache.spark.sql.execution.adaptive

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.joins.{BroadcastHashJoinExec, SortMergeJoinExec}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSQLContext

class AdaptiveQueryExecSuite extends QueryTest with SharedSQLContext {

  setupTestData()

  private def findBroadcastHashJoin(plan: SparkPlan): Seq[SparkPlan] = {
    plan.collect {
      case b: BroadcastHashJoinExec => Seq(b)
      case s: QueryStageExec => findBroadcastHashJoin(s.plan)
    }.flatten
  }

  test("Change merge join to broadcast join") {
    withSQLConf(
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true",
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "80") {
      val df = sql("SELECT * FROM testData join testData2 ON key = a where value = '1'")
      val smj = df.queryExecution.sparkPlan.collect {
        case s: SortMergeJoinExec => s
      }
      assert(smj.size == 1)
      df.collect()
      df.show()
      val adaptivePlan = df.queryExecution.executedPlan.asInstanceOf[AdaptiveSparkPlanExec]
      val bhj = findBroadcastHashJoin(adaptivePlan.executedPlan)
      assert(bhj.nonEmpty, adaptivePlan.executedPlan)
    }
  }

  test("Scalar subquery") {
    withSQLConf(
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true",
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "80") {
      val df = sql("SELECT * FROM testData join testData2 ON key = a " +
        "where value = (SELECT max(a) from testData3)")
      val smj = df.queryExecution.sparkPlan.collect {
        case s: SortMergeJoinExec => s
      }
      assert(smj.size == 1)
      df.collect()
      df.show()
      val adaptivePlan = df.queryExecution.executedPlan.asInstanceOf[AdaptiveSparkPlanExec]
      val bhj = findBroadcastHashJoin(adaptivePlan.executedPlan)
      assert(bhj.size == 1, adaptivePlan.executedPlan)
    }
  }

  test("multiple joins") {
    withSQLConf(
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true",
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "80") {
      val df = sql(
        """
          |WITH t4 AS (
          |  SELECT * FROM lowercaseData t2 JOIN testData3 t3 ON t2.n = t3.a
          |)
          |SELECT * FROM testData
          |JOIN testData2 t2 ON key = t2.a
          |JOIN t4 ON key = t4.a
          |WHERE value = 1
        """.stripMargin)
      val smj = df.queryExecution.sparkPlan.collect {
        case s: SortMergeJoinExec => s
      }
      assert(smj.size == 3)
      df.collect()
      df.show()
      val adaptivePlan = df.queryExecution.executedPlan.asInstanceOf[AdaptiveSparkPlanExec]
      val bhj = findBroadcastHashJoin(adaptivePlan.executedPlan)
      assert(bhj.size == 1, adaptivePlan.executedPlan)
    }
  }

  test("multiple joins with aggregate") {
    withSQLConf(
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true",
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "80") {
      val df = sql(
        """
          |WITH t4 AS (
          |  SELECT * FROM lowercaseData t2 JOIN (
          |    select a, sum(b) from testData3 group by a
          |  ) t3 ON t2.n = t3.a
          |)
          |SELECT * FROM testData
          |JOIN testData2 t2 ON key = t2.a
          |JOIN t4 ON key = t4.a
          |WHERE value = 1
        """.stripMargin)
      val smj = df.queryExecution.sparkPlan.collect {
        case s: SortMergeJoinExec => s
      }
      assert(smj.size == 3)
      df.collect()
      df.show()
      val adaptivePlan = df.queryExecution.executedPlan.asInstanceOf[AdaptiveSparkPlanExec]
      val bhj = findBroadcastHashJoin(adaptivePlan.executedPlan)
      assert(bhj.size == 1, adaptivePlan.executedPlan)
    }
  }

  test("multiple joins with aggregate 2") {
    withSQLConf(
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true",
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "500") {
      val df = sql(
        """
          |WITH t4 AS (
          |  SELECT * FROM lowercaseData t2 JOIN (
          |    select a, max(b) b from testData2 group by a
          |  ) t3 ON t2.n = t3.b
          |)
          |SELECT * FROM testData
          |JOIN testData2 t2 ON key = t2.a
          |JOIN t4 ON value = t4.a
          |WHERE value = 1
        """.stripMargin)
      val smj = df.queryExecution.sparkPlan.collect {
        case s: SortMergeJoinExec => s
      }
      assert(smj.size == 3)
      df.collect()
      df.show()
      val adaptivePlan = df.queryExecution.executedPlan.asInstanceOf[AdaptiveSparkPlanExec]
      val bhj = findBroadcastHashJoin(adaptivePlan.executedPlan)
      assert(bhj.size == 3, adaptivePlan.executedPlan)
    }
  }
}
