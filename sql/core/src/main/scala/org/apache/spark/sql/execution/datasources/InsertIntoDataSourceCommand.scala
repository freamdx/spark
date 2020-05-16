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

package org.apache.spark.sql.execution.datasources

import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.catalyst.plans.QueryPlan
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.execution.command.RunnableCommand
import org.apache.spark.sql.jts.AbstractGeometryUDT
import org.apache.spark.sql.sources.InsertableRelation
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.locationtech.geomesa.spark.jts.util.WKTUtils
import org.locationtech.jts.geom.Geometry

case class InsertProjection(input: StructType, output: StructType) {
  def apply(in: InternalRow): InternalRow = {
    if (input.length != output.length) {
      throw new AnalysisException("Incompatible schema in Insert command...")
    }
    val values = new GenericInternalRow(input.length)
    for (i <- 0 until input.length) {
      val inType = input(i).dataType
      val inValue = in.get(i, inType)
      (inType, output(i).dataType) match {
        case (iType, oType) if iType == oType =>
          values.update(i, inValue)
        case (_, StringType) =>
          values.update(i, UTF8String.fromString(inValue.toString))
        case (StringType, TimestampType) =>
          val ts = DateTimeUtils.stringToTimestamp(inValue.asInstanceOf[UTF8String])
          values.update(i, ts.getOrElse(0L))
        case (StringType, IntegerType) =>
          values.update(i, Integer.parseInt(inValue.toString))
        case (StringType, LongType) =>
          values.update(i, java.lang.Long.parseLong(inValue.toString))
        case (StringType, FloatType) =>
          values.update(i, java.lang.Double.parseDouble(inValue.toString).floatValue())
        case (StringType, DoubleType) =>
          values.update(i, java.lang.Double.parseDouble(inValue.toString))
        case (StringType, gType: AbstractGeometryUDT[Geometry]) =>
          values.update(i, gType.serialize(WKTUtils.read(inValue.toString)))
        case (IntegerType, LongType) =>
          values.update(i, inValue.asInstanceOf[Integer].toLong)
        case (iType, IntegerType) if iType.isInstanceOf[DecimalType] =>
          values.update(i, inValue.asInstanceOf[Decimal].toInt)
        case (iType, LongType) if iType.isInstanceOf[DecimalType] =>
          values.update(i, inValue.asInstanceOf[Decimal].toLong)
        case (iType, FloatType) if iType.isInstanceOf[DecimalType] =>
          values.update(i, inValue.asInstanceOf[Decimal].toFloat)
        case (iType, DoubleType) if iType.isInstanceOf[DecimalType] =>
          values.update(i, inValue.asInstanceOf[Decimal].toDouble)
        case _ =>
          throw new AnalysisException(s"input column ${input(i).name} is incompatible with output column ${output(i).name}")
      }
    }
    values
  }
}

/**
 * Inserts the results of `query` in to a relation that extends [[InsertableRelation]].
 */
case class InsertIntoDataSourceCommand(
    logicalRelation: LogicalRelation,
    query: LogicalPlan,
    overwrite: Boolean)
  extends RunnableCommand {

  override protected def innerChildren: Seq[QueryPlan[_]] = Seq(query)

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val relation = logicalRelation.relation.asInstanceOf[InsertableRelation]
    val data = Dataset.ofRows(sparkSession, query)
    // Data has been casted to the target relation's schema by the PreprocessTableInsertion rule.
    val trans = new InsertProjection(query.schema, logicalRelation.schema)
    val rdd = data.queryExecution.toRdd
    if (!rdd.isEmpty()) {
      val df = sparkSession.internalCreateDataFrame(rdd.map(trans(_)), logicalRelation.schema)
      relation.insert(df, overwrite)
    }

    // Re-cache all cached plans(including this relation itself, if it's cached) that refer to this
    // data source relation.
    sparkSession.sharedState.cacheManager.recacheByPlan(sparkSession, logicalRelation)

    Seq.empty[Row]
  }
}
