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

import org.apache.spark.sql.Strategy
import org.apache.spark.sql.catalyst.expressions.PredicateHelper
import org.apache.spark.sql.catalyst.planning.ExtractEquiJoinKeys
import org.apache.spark.sql.catalyst.plans.logical.{Join, LogicalPlan}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.joins.{BroadcastHashJoinExec, BroadcastNestedLoopJoinExec, BuildLeft, BuildRight}

object LogicalQueryStageStrategy extends Strategy with PredicateHelper {

  private def isBroadcastStage(plan: LogicalPlan): Boolean =
    plan.isInstanceOf[LogicalQueryStage] &&
      plan.asInstanceOf[LogicalQueryStage].physicalPlan.isInstanceOf[BroadcastQueryStageExec]

  def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
    case ExtractEquiJoinKeys(joinType, leftKeys, rightKeys, condition, left, right, hint)
        if isBroadcastStage(left) || isBroadcastStage(right) =>
      val buildSide = if (isBroadcastStage(left)) BuildLeft else BuildRight
      Seq(BroadcastHashJoinExec(
        leftKeys, rightKeys, joinType, buildSide, condition, planLater(left), planLater(right)))

    case j @ Join(left, right, joinType, condition, _)
        if isBroadcastStage(left) || isBroadcastStage(right) =>
      val buildSide = if (isBroadcastStage(left)) BuildLeft else BuildRight
      BroadcastNestedLoopJoinExec(
        planLater(left), planLater(right), buildSide, joinType, condition) :: Nil

    case q: LogicalQueryStage =>
      q.physicalPlan :: Nil

    case _ => Nil
  }
}
