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

package org.apache.spark.sql

import org.apache.spark.sql.catalyst.analysis.TempTableAlreadyExistsException
import org.apache.spark.sql.sedona_sql.UDT.GeometryUDT
import org.apache.spark.sql.test.{SharedSparkSession, SQLTestUtils}
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}

class GeospatialSuite extends QueryTest with SQLTestUtils
  with SharedSparkSession {

  protected override def sparkConf = {
    super.sparkConf
    super.sparkConf
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryo.registrator", "org.apache.sedona.core.serde.SedonaKryoRegistrator")
      .set("spark.sql.extensions", "org.apache.sedona.sql.SedonaSqlExtensions")
  }

  test("cache table as select - existing temp view") {
    withTempView("tempView") {
      sql("CREATE TEMPORARY VIEW tempView as SELECT ST_GeomFromText('POINT(0 0)')")
      val e = intercept[TempTableAlreadyExistsException] {
        sql("CACHE TABLE tempView AS SELECT ST_GeomFromText('POINT(1 1)')")
      }
      assert(e.getMessage.contains("Temporary view 'tempView' already exists"))
    }
  }

  test("CREATE DDL with geometryUDT") {
    withTable("test_geom", "test_geom_1", "test_geom_2") {
      withTempDir { dir =>
        spark.sql(
          s"""
             |CREATE TABLE test_geom (id int not null, geom geometry not null) USING parquet
             |LOCATION '${dir}'
             |""".stripMargin)
        val expectedSchema = StructType(Seq(
          StructField("id", IntegerType),
          StructField("geom", GeometryUDT)
        ))
        assert(spark.table("test_geom").schema == expectedSchema)
      }

      withTempDir { dir =>
        spark.sql(
          s"""
             |CREATE TABLE test_geom_1 USING geoparquet
             |LOCATION '${dir}'
             |AS SELECT ST_GeomFromText('POINT(1 1)')
             |""".stripMargin)
      }
      withTempDir { dir =>
        spark.sql(
          s"""
             |CREATE TABLE test_geom_2 USING orc
             |LOCATION '${dir}'
             |AS SELECT ST_GeomFromText('POINT(1 1)')
             |""".stripMargin)
      }
    }
  }

  test("CREATE DDL with geometryUDT, insert") {
    withTable("test_geom") {
      // 1: create
      withTempDir { dir =>
        spark.sql(
          s"""
             |CREATE TABLE test_geom (id int, geom geometry) USING geoparquet
             |LOCATION '${dir}'
             |""".stripMargin)
        val expectedSchema = StructType(Seq(
          StructField("id", IntegerType, true),
          StructField("geom", GeometryUDT, true)
        ))
        assert(spark.table("test_geom").schema == expectedSchema)
      }

      // 2: insert
      spark.sql(
        s"""
           |INSERT INTO test_geom
           |SELECT 1, ST_GeomFromText('POINT(2 2)')
           |""".stripMargin)
      val df1 = spark.sql("SELECT geom from test_geom").collect()

      spark.sql(
        s"""
           |INSERT OVERWRITE test_geom
           |SELECT 2, ST_GeomFromText('POINT(3 3)')
           |""".stripMargin)
      val df2 = spark.sql("SELECT geom from test_geom").collect()
      assert(df1(0).toString != df2(0).toString)
    }
  }

}
