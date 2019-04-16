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

import scala.collection.mutable
import scala.concurrent.{ExecutionContext, ExecutionContextExecutorService}

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, ReturnAnswer}
import org.apache.spark.sql.catalyst.rules.{Rule, RuleExecutor}
import org.apache.spark.sql.execution.{CollapseCodegenStages, PlanSubqueries, ReuseSubquery, SparkPlan}
import org.apache.spark.sql.execution.exchange.{BroadcastExchangeExec, EnsureRequirements, Exchange, ShuffleExchangeExec}
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.{EventLoop, ThreadUtils}

/**
 * This class inserts [[QueryStageExec]] into the query plan in a bottom-up fashion, and
 * materializes the query stages asynchronously as soon as they are created.
 *
 * When one query stage finishes materialization, a list of adaptive optimizer rules will be
 * executed, trying to optimize the query plan with the data statistics collected from the the
 * materialized data. Then we traverse the query plan again and try to insert more query stages.
 *
 * To create query stages, we traverse the query tree bottom up. When we hit an exchange node,
 * and all the child query stages of this exchange node are materialized, we create a new
 * query stage for this exchange node.
 *
 * Right before the stage creation, a list of query stage optimizer rules will be executed. These
 * optimizer rules are different from the adaptive optimizer rules. Query stage optimizer rules only
 * focus on a plan sub-tree of a specific query stage, and they will be executed only after all the
 * child stages are materialized.
 */
class QueryStageManager(
  initialPlan: SparkPlan,
  session: SparkSession,
  callback: QueryStageManagerCallback)
  extends EventLoop[QueryStageManagerEvent]("QueryStageCreator") {

  private def conf = session.sessionState.conf

  private val readyStages = mutable.HashSet.empty[Int]

  private var currentStageId = 0

  private val stageCache =
    mutable.HashMap.empty[StructType, mutable.Buffer[(Exchange, QueryStageExec)]]

  private var currentPlan = initialPlan

  private var currentLogicalPlan = initialPlan.logicalLink.get

  private val localProperties = session.sparkContext.getLocalProperties

  private implicit def executionContext: ExecutionContextExecutorService = {
    QueryStageManager.executionContext
  }

  // The logical plan optimizer for mid-query re-optimization.
  private object Optimizer extends RuleExecutor[LogicalPlan] {
    // TODO add more optimization rules
    override protected def batches: Seq[Batch] = Seq()
  }

  // A list of rules that will be applied in order to the physical plan before execution.
  private def preparations: Seq[Rule[SparkPlan]] = Seq(
    PlanSubqueries(session),
    EnsureRequirements(session.sessionState.conf),
    ReuseSubquery(session.sessionState.conf))

  // A list of optimizer rules that will be applied right before a query stage is created.
  // These rules need to traverse the plan sub-tree of the query stage to be created, and find
  // chances to optimize this query stage given the all its child query stages.
  private val queryStageOptimizerRules: Seq[Rule[SparkPlan]] = Seq(
    AssertChildStagesMaterialized,
    CollapseCodegenStages(conf))

  private def optimizeEntirePlan(plan: LogicalPlan): SparkPlan = {
    logWarning(s"Re-optimizing plan: $plan")
    val optimized = Optimizer.execute(plan)
    SparkSession.setActiveSession(session)
    val sparkPlan = session.sessionState.planner.plan(ReturnAnswer(optimized)).next()
    logWarning(s"Plan changed to: $sparkPlan")
    preparations.foldLeft(sparkPlan) { case (sp, rule) => rule.apply(sp) }
  }

  private def optimizeQueryStage(plan: SparkPlan): SparkPlan = {
    queryStageOptimizerRules.foldLeft(plan) {
      case (current, rule) => rule(current)
    }
  }

  override protected def onReceive(event: QueryStageManagerEvent): Unit = event match {
    case Start =>
      // set active session and local properties for the event loop thread.
      SparkSession.setActiveSession(session)
      session.sparkContext.setLocalProperties(localProperties)
      createQueryStages(initialPlan)

    case MaterializeStage(stage) =>
      stage.materialize().onComplete { res =>
        if (res.isSuccess) {
          stage.resultOption = Some(res.get)
          post(StageReady(stage))
        } else {
          callback.onStageMaterializationFailed(stage, res.failed.get)
          stop()
        }
      }

    // TODO we should only process the latest StageReady event to avoid waste of re-planning.
    case StageReady(stage) =>
      readyStages += stage.id
      currentPlan = optimizeEntirePlan(currentLogicalPlan)
      createQueryStages(currentPlan)
  }

  override protected def onStart(): Unit = {
    post(Start)
  }

  /**
   * Traverse the query plan bottom-up, and creates query stages as many as possible.
   */
  private def createQueryStages(plan: SparkPlan): SparkPlan = {
    val result = createQueryStages0(plan)
    if (result.allChildStagesReady) {
      val finalPlan = optimizeQueryStage(result.newPlan)
      logWarning(s"Final plan: $finalPlan")
      callback.onFinalPlan(finalPlan)
      finalPlan
    } else {
      currentPlan = result.newPlan
      updateLogicalPlan(result.newStages)
      callback.onPlanUpdate(result.newPlan)
      result.newPlan
    }
  }

  /**
   * This method is called recursively to traverse the plan tree bottom-up. This method returns two
   * information: 1) the new plan after we insert query stages. 2) whether or not the child query
   * stages of the new plan are all ready.
   *
   * if the current plan is an exchange node, and all its child query stages are ready, we create
   * a new query stage.
   */
  private def createQueryStages0(plan: SparkPlan): CreateStageResult = plan match {
    case e: Exchange =>
      val similarStages = stageCache.getOrElseUpdate(e.schema, mutable.Buffer.empty)
      similarStages.find(_._1.sameResult(e)) match {
        case Some((_, existingStage)) if conf.exchangeReuseEnabled =>
          CreateStageResult(
            newPlan = ReusedQueryStageExec(existingStage, e.output),
            allChildStagesReady = readyStages.contains(existingStage.id),
            newStages = Seq.empty)

        case _ =>
          val result = createQueryStages0(e.child)
          val newPlan = e.withNewChildren(Seq(result.newPlan)).asInstanceOf[Exchange]
          // Create a query stage only when all the child query stages are ready.
          if (result.allChildStagesReady) {
            val queryStage = createQueryStage(newPlan)
            similarStages.append(e -> queryStage)
            // We've created a new stage, which is obviously not ready yet.
            CreateStageResult(newPlan = queryStage,
              allChildStagesReady = false, newStages = result.newStages :+ queryStage)
          } else {
            CreateStageResult(newPlan = newPlan,
              allChildStagesReady = false, newStages = result.newStages)
          }
      }

    case q: QueryStageExec =>
      CreateStageResult(newPlan = q,
        allChildStagesReady = readyStages.contains(q.id), newStages = Seq.empty)

    case _ =>
      if (plan.children.isEmpty) {
        CreateStageResult(newPlan = plan, allChildStagesReady = true, newStages = Seq.empty)
      } else {
        val results = plan.children.map(createQueryStages0)
        CreateStageResult(
          newPlan = plan.withNewChildren(results.map(_.newPlan)),
          allChildStagesReady = results.forall(_.allChildStagesReady),
          newStages = results.flatMap(_.newStages))
      }
  }

  private def createQueryStage(e: Exchange): QueryStageExec = {
    val optimizedPlan = optimizeQueryStage(e.child)
    val queryStage = e match {
      case s: ShuffleExchangeExec =>
        ShuffleQueryStageExec(currentStageId, s.copy(child = optimizedPlan))
      case b: BroadcastExchangeExec =>
        BroadcastQueryStageExec(currentStageId, b.copy(child = optimizedPlan))
    }
    // TODO this should have been taken care of by `copy`
    e.logicalLink.foreach(queryStage.plan.setLogicalLink)
    currentStageId += 1
    post(MaterializeStage(queryStage))
    queryStage
  }

  private def updateLogicalPlan(newStages: Seq[QueryStageExec]): Unit = {
    newStages.foreach { s =>
      assert(s.logicalLink.isDefined)
      val logicalNode = s.logicalLink.get
      val physicalNode = currentPlan.collectFirst {
        case p if p.eq(s) || p.logicalLink.exists(logicalNode.eq) => p
      }
      assert(physicalNode.isDefined)
      // Replace the corresponding logical node with LogicalQueryStage
      currentLogicalPlan = currentLogicalPlan.transformDown {
        case p if p eq logicalNode => LogicalQueryStage(logicalNode, physicalNode.get)
      }
    }
  }

  override protected def onError(e: Throwable): Unit = callback.onError(e)
}

case class CreateStageResult(
  newPlan: SparkPlan, allChildStagesReady: Boolean, newStages: Seq[QueryStageExec])

object QueryStageManager {
  private val executionContext = ExecutionContext.fromExecutorService(
    ThreadUtils.newDaemonCachedThreadPool("QueryStageCreator", 16))
}

trait QueryStageManagerCallback {
  def onPlanUpdate(updatedPlan: SparkPlan): Unit
  def onFinalPlan(finalPlan: SparkPlan): Unit
  def onStageMaterializationFailed(stage: QueryStageExec, e: Throwable): Unit
  def onError(e: Throwable): Unit
}

sealed trait QueryStageManagerEvent

object Start extends QueryStageManagerEvent

case class MaterializeStage(stage: QueryStageExec) extends QueryStageManagerEvent

case class StageReady(stage: QueryStageExec) extends QueryStageManagerEvent
