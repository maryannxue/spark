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
import org.apache.spark.sql.execution.joins.{BroadcastHashJoinExec, SortMergeJoinExec}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSQLContext

class AdaptiveQueryExecSuite extends QueryTest with SharedSQLContext {

  setupTestData()

  test("Change merge join to broadcast join") {
    withSQLConf(
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true",
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "80") {
      val df = sql("SELECT * FROM testData join testData2 ON key = a where value = '1'")
      val smj = df.queryExecution.sparkPlan.collect {
        case s: SortMergeJoinExec => s
      }
      assert(smj.nonEmpty)
      df.collect()
      df.show()
      val adaptivePlan = df.queryExecution.executedPlan.asInstanceOf[AdaptiveSparkPlanExec]
      val bhj = adaptivePlan.executedPlan.collect {
        case b: BroadcastHashJoinExec => b
      }
      assert(bhj.nonEmpty, adaptivePlan.executedPlan)
    }
  }
}
