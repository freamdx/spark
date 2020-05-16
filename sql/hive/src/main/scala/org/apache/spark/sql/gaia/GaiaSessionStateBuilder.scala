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

package org.apache.spark.sql.gaia

import org.apache.spark.annotation.{Experimental, InterfaceStability}
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.analysis.Analyzer
import org.apache.spark.sql.catalyst.catalog.{ExternalCatalogWithListener, SessionCatalog}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.SparkPlanner
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.internal.{BaseSessionStateBuilder, SessionState}

@Experimental
@InterfaceStability.Unstable
class GaiaSessionStateBuilder(session: SparkSession, parentState: Option[SessionState] = None)
  extends BaseSessionStateBuilder(session, parentState) {

  private def externalCatalog: ExternalCatalogWithListener = session.sharedState.externalCatalog

  override protected lazy val catalog: SessionCatalog = {
    val catalog = new SessionCatalog(
      () => externalCatalog,
      () => session.sharedState.globalTempViewManager,
      functionRegistry,
      conf,
      SessionState.newHadoopConf(session.sparkContext.hadoopConfiguration, conf),
      sqlParser,
      resourceLoader)
    parentState.foreach(_.catalog.copyStateTo(catalog))
    catalog
  }

  override protected def analyzer: Analyzer = new Analyzer(catalog, conf) {
    override val extendedResolutionRules: Seq[Rule[LogicalPlan]] =
      new FindDataSourceTable(session) +:
        customResolutionRules

    override val postHocResolutionRules: Seq[Rule[LogicalPlan]] =
      DataSourceAnalysis(conf) +:
        GaiaAnalysis +:
        customPostHocResolutionRules

    override val extendedCheckRules: Seq[LogicalPlan => Unit] =
      PreWriteCheck +:
        PreReadCheck +:
        customCheckRules
  }

  override protected def planner: SparkPlanner = {
    new SparkPlanner(session.sparkContext, conf, experimentalMethods) with GaiaStrategies {
      override val sparkSession: SparkSession = session

      override def extraPlanningStrategies: Seq[Strategy] =
        super.extraPlanningStrategies ++ customPlanningStrategies

      override def strategies: Seq[Strategy] = {
        experimentalMethods.extraStrategies ++
          extraPlanningStrategies ++ Seq(
          FileSourceStrategy,
          DataSourceStrategy(conf),
          SpecialLimits,
          InMemoryScans,
          HBaseDataScan,
          Aggregation,
          JoinSelection,
          BasicOperators
        )
      }
    }
  }

  override protected def newBuilder: NewBuilder = new GaiaSessionStateBuilder(_, _)
}
