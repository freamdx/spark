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

import java.lang.reflect.InvocationTargetException
import java.util.Locale
import java.util.concurrent.locks.{Lock, ReentrantReadWriteLock}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HConstants
import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.analysis.{NoSuchTableException, TableAlreadyExistsException}
import org.apache.spark.sql.catalyst.catalog._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.{FunctionIdentifier, TableIdentifier}
import org.apache.spark.sql.types.StructType
import org.geotools.data.DataStoreFinder
import org.geotools.data.simple.SimpleFeatureStore
import org.locationtech.geomesa.spark.GeoMesaDataSource
import org.opengis.filter.Filter

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.util.control.NonFatal

private[spark] class GaiaExternalCatalog(conf: SparkConf, hadoopConf: Configuration)
  extends ExternalCatalog with Logging {

  import CatalogTypes.TablePartitionSpec

  private val tmpParams = collection.mutable.Map[String, String]()
  if (conf.contains("spark.accumulo.zookeepers")) {
    tmpParams.put("accumulo.zookeepers", conf.get("spark.accumulo.zookeepers"))
    tmpParams.put("accumulo.instance.id", conf.get("spark.accumulo.instanceId", "stdb"))
    tmpParams.put("accumulo.catalog", conf.get("spark.accumulo.catalog", "gaia"))
    tmpParams.put("accumulo.user", conf.get("spark.accumulo.user", "root"))
    tmpParams.put("accumulo.password", conf.get("spark.accumulo.password", "1234"))
  }
  if (conf.contains("spark.hbase.zookeepers")) {
    tmpParams.put("hbase.zookeepers", conf.get("spark.hbase.zookeepers"))
    tmpParams.put("hbase.catalog", conf.get("spark.hbase.catalog", "gaia"))
    tmpParams.put("hbase.zookeeper.znode", conf.get("spark.hbase.znode", "/hbase"))
    tmpParams.put("hbase.group", conf.get("spark.hbase.group", "stdb"))
    tmpParams.put("hbase.compression", conf.get("spark.hbase.compression", "zstd"))
    tmpParams.put("hbase.ttl", conf.get("spark.hbase.ttl", HConstants.FOREVER.toString))
  }
  if (conf.contains("spark.jdbc.dbtype")) {
    tmpParams.put("geotools", conf.get("spark.geotools", "true"))
    tmpParams.put("dbtype", conf.get("spark.jdbc.dbtype", "postgis"))
    tmpParams.put("database", conf.get("spark.jdbc.database", "postgres"))
    tmpParams.put("host", conf.get("spark.jdbc.host", "localhost"))
    tmpParams.put("port", conf.get("spark.jdbc.port", "5432"))
    tmpParams.put("user", conf.get("spark.jdbc.user", "root"))
    tmpParams.put("passwd", conf.get("spark.jdbc.password", "1234"))
    tmpParams.put("max connections", conf.get("spark.jdbc.maxConn", "50"))
    tmpParams.put("Max open prepared statements", conf.get("spark.jdbc.maxStatements", "1000"))
    tmpParams.put("fetch size", conf.get("spark.jdbc.fetchSize", "1000"))
    tmpParams.put("Batch insert size", conf.get("spark.jdbc.batchSize", "1000"))
    if (conf.contains("spark.jdbc.useStatements")) tmpParams.put("preparedStatements", conf.get("spark.jdbc.useStatements"))
    if (conf.contains("spark.jdbc.storageEngine")) tmpParams.put("storage engine", conf.get("spark.jdbc.storageEngine"))
  }
  if (conf.contains("spark.gaia.cache")) tmpParams.put("cache", conf.get("spark.gaia.cache"))
  if (conf.contains("spark.gaia.indexGeom")) tmpParams.put("indexGeom", conf.get("spark.gaia.indexGeom"))
  if (conf.contains("spark.gaia.spatial")) tmpParams.put("spatial", conf.get("spark.gaia.spatial"))
  if (conf.contains("spark.gaia.partitions")) tmpParams.put("partitions", conf.get("spark.gaia.partitions"))
  if (conf.contains("spark.gaia.strategy")) tmpParams.put("strategy", conf.get("spark.gaia.strategy"))
  if (conf.contains("spark.gaia.cover")) tmpParams.put("cover", conf.get("spark.gaia.cover"))
  if (conf.contains("spark.gaia.gridPartition")) tmpParams.put("gridPartition", conf.get("spark.gaia.gridPartition"))
  if (conf.contains("spark.gaia.gridFactor")) tmpParams.put("gridFactor", conf.get("spark.gaia.gridFactor"))
  if (conf.contains("spark.gaia.geoHashPrecision")) tmpParams.put("geoHashPrecision", conf.get("spark.gaia.geoHashPrecision"))
  if (conf.contains("spark.gaia.lookupJoin")) tmpParams.put("lookupJoin", conf.get("spark.gaia.lookupJoin"))
  if (conf.contains("spark.gaia.showFid")) tmpParams.put("showFid", conf.get("spark.gaia.showFid"))

  private val params = tmpParams.toMap
  private val ds = DataStoreFinder.getDataStore(params.asJava)
  private val lock = new ReentrantReadWriteLock()

  /**
    * Run some code involving `client` in a [[synchronized]] block and wrap certain
    * exceptions thrown in the process in [[AnalysisException]].
    */
  private def withClient[T](lock: Lock)(body: => T): T = {
    try {
      lock.lock()
      body
    } catch {
      case NonFatal(exception) =>
        val e = exception match {
          case i: InvocationTargetException => i.getCause
          case o => o
        }
        throw new AnalysisException(
          e.getClass.getCanonicalName + ": " + e.getMessage, cause = Some(e))
    } finally {
      lock.unlock()
    }
  }

  // --------------------------------------------------------------------------
  // Databases
  // --------------------------------------------------------------------------

  override def createDatabase(dbDefinition: CatalogDatabase,
                              ignoreIfExists: Boolean): Unit = withClient(lock.writeLock()) {
  }

  override def dropDatabase(db: String,
                            ignoreIfNotExists: Boolean,
                            cascade: Boolean): Unit = withClient(lock.writeLock()) {
    throw new NotImplementedError()
  }

  override def alterDatabase(dbDefinition: CatalogDatabase): Unit = withClient(lock.writeLock()) {
    throw new NotImplementedError()
  }

  override def getDatabase(db: String): CatalogDatabase = withClient(lock.readLock()) {
    requireDbExists(db)
    CatalogDatabase(db, "default spatio-temporal database",
      CatalogUtils.stringToURI("spatio-temporal"), params)
  }

  override def databaseExists(db: String): Boolean = withClient(lock.readLock()) {
    listDatabases.exists(d => d.equals(db))
  }

  override def listDatabases(): Seq[String] = withClient(lock.readLock()) {
    listDatabases("*")
  }

  override def listDatabases(pattern: String): Seq[String] = withClient(lock.readLock()) {
    Seq(SessionCatalog.DEFAULT_DATABASE)
  }

  override def setCurrentDatabase(db: String): Unit = withClient(lock.readLock()) {
    requireDbExists(db)
  }

  // --------------------------------------------------------------------------
  // Tables
  // --------------------------------------------------------------------------

  override def createTable(tableDefinition: CatalogTable,
                           ignoreIfExists: Boolean): Unit = withClient(lock.writeLock()) {
    assert(tableDefinition.identifier.database.isDefined)
    val db = tableDefinition.identifier.database.get
    val table = tableDefinition.identifier.table
    requireDbExists(db)
    verifyColumnNames(tableDefinition)

    if (tableExists(db, table) && !ignoreIfExists) {
      throw new TableAlreadyExistsException(db, table)
    }

    val sft = new GeoMesaDataSource().structType2SFT(tableDefinition.schema, table)
    tableDefinition.properties.foreach { case (key, value) =>
      sft.getUserData.put(key, value)
    }
    ds.createSchema(sft)
  }

  private def verifyColumnNames(table: CatalogTable): Unit = {
    table.dataSchema.map(_.name).foreach { col =>
      if (col.contains(",")) {
        throw new AnalysisException(s"Table: ${table.identifier} Column: $col contains ','")
      }
    }
  }

  override def dropTable(db: String,
                         table: String,
                         ignoreIfNotExists: Boolean,
                         purge: Boolean): Unit = withClient(lock.writeLock()) {
    requireTableExists(db, table)
    ds.removeSchema(table)
  }

  override def renameTable(db: String,
                           oldName: String,
                           newName: String): Unit = withClient(lock.writeLock()) {
    throw new NotImplementedError()
  }

  override def alterTable(tableDefinition: CatalogTable): Unit = withClient(lock.writeLock()) {
    assert(tableDefinition.identifier.database.isDefined)
    val db = tableDefinition.identifier.database.get
    requireTableExists(db, tableDefinition.identifier.table)
    throw new NotImplementedError()
  }

  override def alterTableDataSchema(db: String,
                                    table: String,
                                    newDataSchema: StructType): Unit = withClient(lock.writeLock()) {
    requireTableExists(db, table)
    throw new NotImplementedError()
  }

  override def alterTableStats(db: String,
                               table: String,
                               stats: Option[CatalogStatistics]): Unit = withClient(lock.writeLock()) {
    requireTableExists(db, table)
    throw new NotImplementedError()
  }

  override def getTable(db: String, table: String): CatalogTable = withClient(lock.readLock()) {
    requireTableExists(db, table)
    val sft = ds.getSchema(table)
    if (sft == null) throw new NoSuchTableException(db, table)
    val showFid = params.getOrDefault("showFid", "false").equals("true")
    val schema = new GeoMesaDataSource().sft2StructType(sft, showFid)
    val parameters = params + ("geomesa.feature" -> table)
    val format = CatalogStorageFormat.empty.copy(properties = parameters)
    val properties = collection.mutable.Map[String, String]()
    sft.getUserData.foreach { case (k: String, v: String) =>
      properties.put(k, v)
    }
    CatalogTable(TableIdentifier(table, Some(db)), CatalogTableType.EXTERNAL,
      format, schema, Some("geomesa"), properties = properties.toMap)
  }

  override def tableExists(db: String, table: String): Boolean = withClient(lock.readLock()) {
    requireDbExists(db)
    val schema = try {
      ds.getSchema(table)
    } catch {
      case _ => null
    }
    schema != null
  }

  override def listTables(db: String): Seq[String] = withClient(lock.readLock()) {
    requireDbExists(db)
    listTables(db, "*")
  }

  override def listTables(db: String, pattern: String): Seq[String] = withClient(lock.readLock()) {
    requireDbExists(db)
    ds.getTypeNames.toSeq
  }

  override def loadTable(db: String,
                         table: String,
                         loadPath: String,
                         isOverwrite: Boolean,
                         isSrcLocal: Boolean): Unit = withClient(lock.readLock()) {
    requireTableExists(db, table)
    throw new NotImplementedError()
  }

  override def truncateTable(db: String,
                             table: String): Unit = withClient(lock.writeLock()) {
    requireTableExists(db, table)
    val featureSource = ds.getFeatureSource(table)
    if (!featureSource.isInstanceOf[SimpleFeatureStore]) {
      throw new AnalysisException("truncateTable unsupported...")
    }
    featureSource.asInstanceOf[SimpleFeatureStore].removeFeatures(Filter.INCLUDE)
  }

  override def loadPartition(db: String,
                             table: String,
                             loadPath: String,
                             partition: TablePartitionSpec,
                             isOverwrite: Boolean,
                             inheritTableSpecs: Boolean,
                             isSrcLocal: Boolean): Unit = withClient(lock.readLock()) {
    requireTableExists(db, table)
    throw new NotImplementedError()
  }

  override def loadDynamicPartitions(db: String,
                                     table: String,
                                     loadPath: String,
                                     partition: TablePartitionSpec,
                                     replace: Boolean,
                                     numDP: Int): Unit = withClient(lock.readLock()) {
    requireTableExists(db, table)
    throw new NotImplementedError()
  }

  // --------------------------------------------------------------------------
  // Partitions
  // --------------------------------------------------------------------------

  override def createPartitions(db: String,
                                table: String,
                                parts: Seq[CatalogTablePartition],
                                ignoreIfExists: Boolean): Unit = withClient(lock.writeLock()) {
    requireTableExists(db, table)
    throw new NotImplementedError()
  }

  override def dropPartitions(db: String,
                              table: String,
                              parts: Seq[TablePartitionSpec],
                              ignoreIfNotExists: Boolean,
                              purge: Boolean,
                              retainData: Boolean): Unit = withClient(lock.writeLock()) {
    requireTableExists(db, table)
    throw new NotImplementedError()
  }

  override def renamePartitions(db: String,
                                table: String,
                                specs: Seq[TablePartitionSpec],
                                newSpecs: Seq[TablePartitionSpec]): Unit = withClient(lock.writeLock()) {
    throw new NotImplementedError()
  }

  override def alterPartitions(db: String,
                               table: String,
                               newParts: Seq[CatalogTablePartition]): Unit = withClient(lock.writeLock()) {
    throw new NotImplementedError()
  }

  override def getPartition(db: String,
                            table: String,
                            spec: TablePartitionSpec): CatalogTablePartition = withClient(lock.readLock()) {
    throw new NotImplementedError()
  }

  override def getPartitionOption(db: String,
                                  table: String,
                                  spec: TablePartitionSpec): Option[CatalogTablePartition] = withClient(lock.readLock()) {
    throw new NotImplementedError()
  }

  override def listPartitionNames(db: String,
                                  table: String,
                                  partialSpec: Option[TablePartitionSpec] = None): Seq[String] = withClient(lock.readLock()) {
    throw new NotImplementedError()
  }

  override def listPartitions(db: String,
                              table: String,
                              partialSpec: Option[TablePartitionSpec] = None): Seq[CatalogTablePartition] = withClient(lock.readLock()) {
    throw new NotImplementedError()
  }

  override def listPartitionsByFilter(db: String,
                                      table: String,
                                      predicates: Seq[Expression],
                                      defaultTimeZoneId: String): Seq[CatalogTablePartition] = withClient(lock.readLock()) {
    throw new NotImplementedError()
  }

  // --------------------------------------------------------------------------
  // Functions
  // --------------------------------------------------------------------------

  override def createFunction(db: String,
                              funcDefinition: CatalogFunction): Unit = withClient(lock.writeLock()) {
    requireDbExists(db)
    throw new NotImplementedError()
  }

  override def dropFunction(db: String, name: String): Unit = withClient(lock.writeLock()) {
    requireFunctionExists(db, name)
    throw new NotImplementedError()
  }

  override def alterFunction(db: String,
                             funcDefinition: CatalogFunction): Unit = withClient(lock.writeLock()) {
    requireDbExists(db)
    val functionName = funcDefinition.identifier.funcName.toLowerCase(Locale.ROOT)
    requireFunctionExists(db, functionName)
    throw new NotImplementedError()
  }

  override def renameFunction(db: String,
                              oldName: String,
                              newName: String): Unit = withClient(lock.writeLock()) {
    requireFunctionExists(db, oldName)
    requireFunctionNotExists(db, newName)
    throw new NotImplementedError()
  }

  override def getFunction(db: String,
                           funcName: String): CatalogFunction = withClient(lock.readLock()) {
    CatalogFunction(FunctionIdentifier("udf"), "org.apache", Seq.empty)
  }

  override def functionExists(db: String,
                              funcName: String): Boolean = withClient(lock.readLock()) {
    requireDbExists(db)
    throw new NotImplementedError()
  }

  override def listFunctions(db: String,
                             pattern: String): Seq[String] = withClient(lock.readLock()) {
    requireDbExists(db)
    throw new NotImplementedError()
  }
}


