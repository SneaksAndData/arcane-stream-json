package com.sneaksanddata.arcane.stream_json
package tests

import models.app.JsonPluginStreamContext
import tests.Common.{avroSchemaString, nestedAvroSchemaString}

import com.sneaksanddata.arcane.framework.models.schemas.ArcaneType.{IntType, LongType, StringType}
import com.sneaksanddata.arcane.framework.models.schemas.{ArcaneSchema, IndexedField, IndexedMergeKeyField}
import com.sneaksanddata.arcane.framework.services.blobsource.versioning.BlobSourceWatermark
import com.sneaksanddata.arcane.framework.testkit.setups.FrameworkTestSetup.prepareWatermark
import com.sneaksanddata.arcane.framework.testkit.verifications.FrameworkVerificationUtilities.{
  StrStrDecoder,
  clearTarget,
  getWatermark,
  readTarget
}
import com.sneaksanddata.arcane.framework.testkit.zioutils.ZKit.{liveSeed, runOrFail}
import com.sneaksanddata.arcane.framework.utils.HashUtils
import zio.metrics.connectors.MetricsConfig
import zio.metrics.connectors.datadog.DatadogPublisherConfig
import zio.metrics.connectors.statsd.DatagramSocketConfig
import zio.test.*
import zio.test.TestAspect.timeout
import zio.{Scope, ZIO, ZLayer}

import java.sql.DriverManager
import java.time.Duration
import scala.util.Random

object IntegrationTests extends ZIOSpecDefault:
  val stableSourceBucket       = "s3-blob-reader-json"
  val unstableSourceBucket     = "s3-blob-reader-json-variable"
  val nestedSourceBucket       = "s3-blob-reader-json-nested-array"
  val migrationTargetTableName = "iceberg.test.stream_migration"

  // sbt's layered classloaders hide the JDBC driver from DriverManager's ServiceLoader lookup
  private val registerDriver = ZIO.attempt(java.sql.DriverManager.registerDriver(new io.trino.jdbc.TrinoDriver()))

  private def getStreamContextStr(
      targetTable: String,
      sourceBucket: String,
      schema: String,
      jsonPointerExpr: String,
      jsonArrayPointers: String
  ) =
    s"""
       |{
       |  "observability": {
       |    "metricTags": {}
       |  },
       |  "staging": {
       |    "table": {
       |      "maxRowsPerFile": 10000,
       |      "stagingCatalogName": "iceberg",
       |      "stagingSchemaName": "test",
       |      "isUnifiedSchema": true
       |    },
       |    "icebergCatalog": {
       |      "catalogProperties": {},
       |      "catalogUri": "http://localhost:20001/catalog",
       |      "namespace": "test",
       |      "warehouse": "demo"
       |    }
       |  },
       |  "streamMode": {
       |    "backfill": {
       |      "backfillBehavior": "Overwrite",
       |      "backfillStartDate": "2026-01-01T00:00:00Z"
       |    },
       |    "changeCapture": {
       |      "changeCaptureInterval": "5 second",
       |      "changeCaptureJitterVariance": 0.1,
       |      "changeCaptureJitterSeed": 0,
       |      "changeCaptureRangeLimit": 100
       |    }
       |  },
       |  "sink": {
       |    "mergeServiceClient": {
       |      "connectionUrl": "jdbc:trino://localhost:8080",
       |      "credentialType": {
       |        "basic": {}
       |      },
       |      "extraConnectionParameters": {
       |        "clientTags": "test"
       |      },
       |      "queryRetryMode": {
       |        "never": {}
       |      },
       |      "queryRetryBaseDuration": "100 millisecond",
       |      "queryRetryOnMessageContents": [],
       |      "queryRetryScaleFactor": 0.1,
       |      "queryRetryMaxAttempts": 3
       |    },
       |    "targetTableProperties": {
       |      "format": "PARQUET",
       |      "sortedBy": [],
       |      "parquetBloomFilterColumns": []
       |    },
       |    "targetTableFullName": "$targetTable",
       |    "maintenanceSettings": {
       |      "targetOptimizeSettings": {
       |        "batchThreshold": 60,
       |        "fileSizeThreshold": "512MB"
       |      },
       |      "targetOrphanFilesExpirationSettings": {
       |        "batchThreshold": 60,
       |        "retentionThreshold": "6h"
       |      },
       |      "targetSnapshotExpirationSettings": {
       |        "batchThreshold": 60,
       |        "retentionThreshold": "6h"
       |      },
       |      "targetAnalyzeSettings": {
       |        "includedColumns": [],
       |        "batchThreshold": 60
       |      }
       |    },
       |    "icebergCatalog": {
       |      "catalogProperties": {},
       |      "catalogUri": "http://localhost:20001/catalog",
       |      "namespace": "test",
       |      "warehouse": "demo"
       |    }
       |  },
       |  "throughput": {
       |    "shaperImpl": {
       |      "memoryBound": {
       |        "fallbackStringTypeSizeEstimate": 50,
       |        "objectTypeSizeEstimate": 4096,
       |        "chunkCostScale": 1,
       |        "chunkCostMax": 10,
       |        "tableRowCountWeight": 0.05,
       |        "tableSizeWeight": 0.05,
       |        "tableSizeScaleFactor": 1,
       |        "chunkSizeCap": 1000000,
       |        "maxStatisticsAge": 604800
       |      }
       |    },
       |    "advisedRate": "1000 per 1 second",
       |    "advisedBurst": 1000,
       |    "advisedChunkSize": 10
       |  },
       |  "source": {
       |    "configuration": {
       |      "sourcePath": "s3a://$sourceBucket",
       |      "shardStoragePath": "s3a://tmp",
       |      "tempStoragePath": "/tmp",
       |      "primaryKeys": ["col0"],
       |      "avroSchemaString": "$schema",
       |      "jsonPointerExpression": "$jsonPointerExpr",
       |      "jsonArrayPointers": $jsonArrayPointers,
       |      "s3": {
       |        "usePathStyle": true,
       |        "region": "us-east-1",
       |        "endpoint": "http://localhost:9000",
       |        "maxResultsPerPage": 1000,
       |        "retryMaxAttempts": 5,
       |        "retryBaseDelay": "100 millisecond",
       |        "retryMaxDelay": "1 second"
       |      }
       |    },
       |    "buffering": {
       |      "enabled": false,
       |      "strategy": {}
       |    },
       |    "modifications": {
       |      "supported": {}
       |    }
       |  }
       |}""".stripMargin

  private def getStreamContext(
      tableName: String,
      sourceBucket: String,
      schema: String,
      jsonPointerExpr: String,
      jsonArrayPointers: String
  ): (
      JsonPluginStreamContext,
      ZLayer[Any, Nothing, JsonPluginStreamContext & DatadogPublisherConfig & DatagramSocketConfig & MetricsConfig]
  ) =
    val streamContext = JsonPluginStreamContext(
      getStreamContextStr(tableName, sourceBucket, schema, jsonPointerExpr, jsonArrayPointers)
    )
    (
      streamContext,
      ZLayer.succeed(
        streamContext
      ) ++ ZLayer
        .succeed[DatagramSocketConfig](streamContext) ++ ZLayer
        .succeed[MetricsConfig](streamContext) ++ ZLayer.succeed(DatadogPublisherConfig())
    )

  private def mergeKey(col0: Int): String = HashUtils.murmur3(col0.toString)

  // Legacy sink table schema (framework 2.3) had `createdon` instead of `ARCANE_VERSION`
  private val legacySchema = ArcaneSchema(
    Seq(
      IndexedField(name = "col0", fieldType = IntType, fieldId = 0),
      IndexedField(name = "createdon", fieldType = LongType, fieldId = 1),
      IndexedMergeKeyField(2)
    )
  )

  private val legacyRows = List((1001, 1700000000L), (1002, 1700000001L))

  private def trinoConnectionString: String =
    sys.env.getOrElse(
      "ARCANE_FRAMEWORK__MERGE_SERVICE_CONNECTION_URI",
      "jdbc:trino://localhost:8080/iceberg/test?user=test"
    )

  private def seedLegacyRows(table: String) = ZIO.scoped {
    for
      connection <- ZIO.attempt(DriverManager.getConnection(trinoConnectionString))
      statement  <- ZIO.attempt(connection.createStatement())
      values = legacyRows.map { case (col0, createdOn) => s"($col0, $createdOn, 'stale-key')" }.mkString(", ")
      _ <- ZIO.attemptBlocking(
        statement.execute(s"INSERT INTO $table (col0, createdon, ARCANE_MERGE_KEY) VALUES $values")
      )
    yield ()
  }

  private def readColumns(table: String) = ZIO.scoped {
    for
      connection <- ZIO.attempt(DriverManager.getConnection(trinoConnectionString))
      statement  <- ZIO.attempt(connection.createStatement())
      resultSet  <- ZIO.fromAutoCloseable(ZIO.attemptBlocking(statement.executeQuery(s"DESCRIBE $table")))
      columns <- ZIO.attempt(
        Iterator.continually(resultSet).takeWhile(_.next()).map(_.getString(1).toLowerCase).toList
      )
    yield columns
  }

  override def spec: Spec[TestEnvironment & Scope, Any] = suite("IntegrationTests")(
    test("migrates legacy sink table to ARCANE_MERGE_KEY and ARCANE_VERSION and streams") {
      for
        _ <- clearTarget(migrationTargetTableName)
        _ <- prepareWatermark(migrationTargetTableName.split('.').last, legacySchema, BlobSourceWatermark.epoch)
        _ <- seedLegacyRows(migrationTargetTableName)

        columnsBefore <- readColumns(migrationTargetTableName)

        (migrationContext, migrationContextLayer) = getStreamContext(
          migrationTargetTableName,
          stableSourceBucket,
          avroSchemaString,
          "",
          "{}"
        )

        _        <- TestSystem.putEnv("STREAMCONTEXT__RUNMODE", "migrate")
        migrator <- Common.getTestApp(Duration.ofSeconds(45), migrationContextLayer).fork
        _        <- migrator.runOrFail(Duration.ofSeconds(30))
        _        <- TestSystem.putEnv("STREAMCONTEXT__RUNMODE", "stream")

        columnsAfter <- readColumns(migrationTargetTableName)
        migrated <- readTarget(
          migrationContext.sink.targetTableFullName,
          "ARCANE_MERGE_KEY, ARCANE_VERSION",
          StrStrDecoder
        )

        // Now run the stream on the migrated table to verify stream ingestion works on it
        streamRunner <- Common.getTestApp(Duration.ofSeconds(25), migrationContextLayer).fork
        _            <- streamRunner.runOrFail(Duration.ofSeconds(20))

        migratedStreamWatermark <- getWatermark(migrationContext.sink.targetTableFullName.split('.').last)(using
          BlobSourceWatermark.rw
        )
      yield assertTrue(columnsBefore.contains("createdon")) implies
        assertTrue(!columnsBefore.contains("arcane_version")) implies
        assertTrue(!columnsAfter.contains("createdon")) implies
        assertTrue(columnsAfter.contains("arcane_merge_key") && columnsAfter.contains("arcane_version")) implies
        assertTrue(
          migrated.sorted == legacyRows.map { case (col0, createdOn) => (mergeKey(col0), createdOn.toString) }.sorted
        ) implies
        assertTrue(migratedStreamWatermark.version.toLong > 0)
    },
    test("runs backfill from a stable JSON source - file schema identical") {
      for
        _         <- TestSystem.putEnv("STREAMCONTEXT__BACKFILL", "true")
        _         <- TestSystem.putEnv("STREAMCONTEXT__BACKFILL_ID", Random.alphanumeric.take(10).mkString(""))
        tableName <- ZIO.succeed("iceberg.test.stream_stable_identical_schema")
        _         <- ZIO.attempt(clearTarget(tableName))
        (streamContext, streamContextLayer) = getStreamContext(
          tableName,
          stableSourceBucket,
          avroSchemaString,
          "",
          "{}"
        )
        backfillRunner <- Common.getTestApp(Duration.ofSeconds(60), streamContextLayer).fork
        _              <- backfillRunner.runOrFail(Duration.ofSeconds(45))
        target <- readTarget(
          tableName,
          "col0, col1, col2, col3, col4, col5, col6, col7, col8, col9, arcane_merge_key, arcane_version",
          Common.TargetDecoder
        )
      yield assertTrue(target.size == 100) // col0 only have 100 unique values, thus we expect 100 rows total
    },
    test("runs stream correctly from a stable JSON source - file schema identical") {
      for
        tableName <- ZIO.succeed("iceberg.test.stream_stable_identical_schema")
        (streamContext, streamContextLayer) = getStreamContext(
          tableName,
          stableSourceBucket,
          avroSchemaString,
          "",
          "{}"
        )
        streamRunner <- Common.getTestApp(Duration.ofSeconds(60), streamContextLayer).fork
        _            <- streamRunner.runOrFail(Duration.ofSeconds(45))
        rows <- readTarget(
          tableName,
          "col0, col1, col2, col3, col4, col5, col6, col7, col8, col9, arcane_merge_key, arcane_version",
          Common.TargetDecoder
        )
      yield assertTrue(rows.size == 100) // no new rows added after stream has started
    },
    test("runs backfill from an unstable JSON source - file schema varies from file to file") {
      for
        tableName <- ZIO.succeed("iceberg.test.stream_varying_schema")
        (streamContext, streamContextLayer) = getStreamContext(
          tableName,
          unstableSourceBucket,
          avroSchemaString,
          "",
          "{}"
        )
        _              <- TestSystem.putEnv("STREAMCONTEXT__BACKFILL", "true")
        _              <- TestSystem.putEnv("STREAMCONTEXT__BACKFILL_ID", Random.alphanumeric.take(10).mkString(""))
        _              <- ZIO.attempt(clearTarget(tableName))
        backfillRunner <- Common.getTestApp(Duration.ofSeconds(60), streamContextLayer).fork
        _              <- backfillRunner.runOrFail(Duration.ofSeconds(45))
        rows <- readTarget(
          tableName,
          "col0, col1, col2, col3, col4, col5, col6, col7, col8, col9, arcane_merge_key, arcane_version",
          Common.TargetDecoder
        )
      yield assertTrue(rows.size == 100) // col0 only have 100 unique values, thus we expect 100 rows total
    },
    test("runs stream correctly from an unstable JSON source - file schema varies from file to file") {
      for
        tableName <- ZIO.succeed("iceberg.test.stream_varying_schema")
        (streamContext, streamContextLayer) = getStreamContext(
          tableName,
          unstableSourceBucket,
          avroSchemaString,
          "",
          "{}"
        )
        _ <- prepareWatermark(
          tableName.split("\\.").last,
          ArcaneSchema(Seq(IndexedMergeKeyField(0))),
          BlobSourceWatermark.epoch
        )
        streamRunner <- Common.getTestApp(Duration.ofSeconds(60), streamContextLayer).fork
        _            <- streamRunner.runOrFail(Duration.ofSeconds(45))
        rows <- readTarget(
          tableName,
          "col0, col1, col2, col3, col4, col5, col6, col7, col8, col9, arcane_merge_key, arcane_version",
          Common.TargetDecoder
        )
      yield assertTrue(rows.size == 100) // no new rows added after stream has started
    },
    test("runs backfill from a JSON source - files contain nested array") {
      for
        tableName <- ZIO.succeed("iceberg.test.nested_schema")
        (streamContext, streamContextLayer) = getStreamContext(
          tableName,
          nestedSourceBucket,
          nestedAvroSchemaString,
          "/body",
          "{ \"/nested_array/value\": {} }"
        )
        _              <- TestSystem.putEnv("STREAMCONTEXT__BACKFILL", "true")
        _              <- TestSystem.putEnv("STREAMCONTEXT__BACKFILL_ID", Random.alphanumeric.take(10).mkString(""))
        _              <- ZIO.attempt(clearTarget(tableName))
        backfillRunner <- Common.getTestApp(Duration.ofSeconds(60), streamContextLayer).fork
        _              <- backfillRunner.runOrFail(Duration.ofSeconds(45))
        rows <- readTarget(
          tableName,
          "col0, col1, col2, col3, col4, col5, col6, col7, col8, col9, nested_col_1, nested_col_2, arcane_merge_key, arcane_version",
          Common.TargetNestedDecoder
        )
      yield assertTrue(rows.size == 100) // col0 only have 100 unique values, thus we expect 100 rows total
    },
    test("runs stream correctly from a nested JSON source - file schema contains nested arrays") {
      for
        tableName <- ZIO.succeed("iceberg.test.nested_schema")
        (streamContext, streamContextLayer) = getStreamContext(
          tableName,
          nestedSourceBucket,
          nestedAvroSchemaString,
          "/body",
          "{ \"/nested_array/value\": {} }"
        )
        _ <- prepareWatermark(
          tableName.split("\\.").last,
          ArcaneSchema(Seq(IndexedMergeKeyField(0))),
          BlobSourceWatermark.epoch
        )
        streamRunner <- Common.getTestApp(Duration.ofSeconds(60), streamContextLayer).fork
        _            <- streamRunner.runOrFail(Duration.ofSeconds(45))
        rows <- readTarget(
          tableName,
          "col0, col1, col2, col3, col4, col5, col6, col7, col8, col9, nested_col_1, nested_col_2, arcane_merge_key, arcane_version",
          Common.TargetNestedDecoder
        )
      yield assertTrue(rows.size == 100) // no new rows added after stream has started
    }
  ) @@ timeout(zio.Duration.fromSeconds(240)) @@ TestAspect.withLiveClock @@ TestAspect.sequential @@ TestAspect.before(
    registerDriver *> liveSeed
  )
