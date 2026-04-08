package com.sneaksanddata.arcane.stream_json
package tests

import models.app.JsonPluginStreamContext
import tests.Common.{avroSchemaString, nestedAvroSchemaString}

import com.sneaksanddata.arcane.framework.models.schemas.{ArcaneSchema, MergeKeyField}
import com.sneaksanddata.arcane.framework.services.blobsource.versioning.BlobSourceWatermark
import com.sneaksanddata.arcane.framework.testkit.setups.FrameworkTestSetup.prepareWatermark
import com.sneaksanddata.arcane.framework.testkit.verifications.FrameworkVerificationUtilities.{clearTarget, readTarget}
import com.sneaksanddata.arcane.framework.testkit.zioutils.ZKit.{liveSeed, runOrFail}
import zio.test.*
import zio.test.TestAspect.timeout
import zio.{Scope, ZIO, ZLayer}

import java.time.Duration

object IntegrationTests extends ZIOSpecDefault:
  val targetTableName = "iceberg.test.stream_run"

  val stableSourceBucket   = "s3-blob-reader-json"
  val unstableSourceBucket = "s3-blob-reader-json-variable"

  val nestedSourceBucket    = "s3-blob-reader-json-nested-array"
  var targetTableNameNested = "iceberg.test.stream_nested_run"

  private def getStreamContextStr(
      targetTable: String,
      sourceBucket: String,
      schema: String,
      jsonPointerExpr: String,
      jsonArrayPointers: String
  ) =
    s"""
       |{
       |  "backfillJobTemplateRef": {
       |    "apiGroup": "streaming.sneaksanddata.com",
       |    "kind": "StreamingJobTemplate",
       |    "name": "arcane-stream-parquet-large-job"
       |  },
       |  "jobTemplateRef": {
       |    "apiGroup": "streaming.sneaksanddata.com",
       |    "kind": "StreamingJobTemplate",
       |    "name": "arcane-stream-parquet-standard-job"
       |  },
       |  "observability": {
       |    "metricTags": {}
       |  },
       |  "staging": {
       |    "table": {
       |      "stagingTablePrefix": "staging_${targetTable.replace(".", "_")}",
       |      "maxRowsPerFile": 10000,
       |      "stagingCatalogName": "iceberg",
       |      "stagingSchemaName": "test",
       |      "isUnifiedSchema": false
       |    },
       |    "icebergCatalog": {
       |      "catalogProperties": {},
       |      "catalogUri": "http://localhost:20001/catalog",
       |      "namespace": "test",
       |      "warehouse": "demo",
       |      "maxCatalogInstanceLifetime": "3600 second"
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
       |      "changeCaptureJitterSeed": 0
       |    }
       |  },
       |  "sink": {
       |    "mergeServiceClient": {
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
       |      "warehouse": "demo",
       |      "maxCatalogInstanceLifetime": "3600 second"
       |    }
       |  },
       |  "throughput": {
       |    "shaperImpl": {
       |      "memoryBound": {
       |        "meanStringTypeSizeEstimate": 500,
       |        "meanObjectTypeSizeEstimate": 4096,
       |        "burstEstimateDivisionFactor": 2,
       |        "rateEstimateDivisionFactor": 2,
       |        "chunkCostScale": 1,
       |        "chunkCostMax": 10,
       |        "tableRowCountWeight": 0.05,
       |        "tableSizeWeight": 0.09,
       |        "tableSizeScaleFactor": 1
       |      }
       |    },
       |    "advisedRatePeriod": "1 second",
       |    "advisedChunksBurst": 1000,
       |    "advisedChunkSize": 10,
       |    "advisedRateChunks": 1000
       |  },
       |  "source": {
       |    "configuration": {
       |      "sourcePath": "s3a://$sourceBucket",
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
       |    "fieldSelectionRule": {
       |      "essentialFields": [],
       |      "rule":{
       |        "all": {}
       |      },
       |      "isServerSide": false
       |    }
       |  }
       |}""".stripMargin

  private val stableStreamContext = JsonPluginStreamContext(
    getStreamContextStr(targetTableName, stableSourceBucket, avroSchemaString, "", "{}")
  )
  private val stableStreamContextLayer = ZLayer.succeed(stableStreamContext)

  private val unstableStreamContext = JsonPluginStreamContext(
    getStreamContextStr(targetTableName, unstableSourceBucket, avroSchemaString, "", "{}")
  )
  private val unstableStreamContextLayer = ZLayer.succeed(unstableStreamContext)

  private val nestedStreamContext = JsonPluginStreamContext(
    getStreamContextStr(
      targetTableNameNested,
      nestedSourceBucket,
      nestedAvroSchemaString,
      "/body",
      "{ \"/nested_array/value\": {} }"
    )
  )
  private val nestedStreamContextLayer = ZLayer.succeed(nestedStreamContext)

  override def spec: Spec[TestEnvironment & Scope, Any] = suite("IntegrationTests")(
    test("runs backfill from a stable JSON source - file schema identical") {
      for
        _              <- TestSystem.putEnv("STREAMCONTEXT__BACKFILL", "true")
        _              <- ZIO.attempt(clearTarget(targetTableName))
        backfillRunner <- Common.getTestApp(Duration.ofSeconds(60), stableStreamContextLayer).fork
        _              <- backfillRunner.runOrFail(Duration.ofSeconds(45))
        target <- readTarget(
          stableStreamContext.sink.targetTableFullName,
          "col0, col1, col2, col3, col4, col5, col6, col7, col8, col9, arcane_merge_key, createdon",
          Common.TargetDecoder
        )
      yield assertTrue(target.size == 100) // col0 only have 100 unique values, thus we expect 100 rows total
    },
    test("runs stream correctly from a stable JSON source - file schema identical") {
      for
        streamRunner <- Common.getTestApp(Duration.ofSeconds(60), stableStreamContextLayer).fork
        _            <- streamRunner.runOrFail(Duration.ofSeconds(45))
        rows <- readTarget(
          stableStreamContext.sink.targetTableFullName,
          "col0, col1, col2, col3, col4, col5, col6, col7, col8, col9, arcane_merge_key, createdon",
          Common.TargetDecoder
        )
      yield assertTrue(rows.size == 100) // no new rows added after stream has started
    },
    test("runs backfill from an unstable JSON source - file schema varies from file to file") {
      for
        _              <- TestSystem.putEnv("STREAMCONTEXT__BACKFILL", "true")
        _              <- ZIO.attempt(clearTarget(targetTableName))
        backfillRunner <- Common.getTestApp(Duration.ofSeconds(60), unstableStreamContextLayer).fork
        _              <- backfillRunner.runOrFail(Duration.ofSeconds(45))
        rows <- readTarget(
          unstableStreamContext.sink.targetTableFullName,
          "col0, col1, col2, col3, col4, col5, col6, col7, col8, col9, arcane_merge_key, createdon",
          Common.TargetDecoder
        )
      yield assertTrue(rows.size == 100) // col0 only have 100 unique values, thus we expect 100 rows total
    },
    test("runs stream correctly from an unstable JSON source - file schema varies from file to file") {
      for
        _ <- prepareWatermark(
          targetTableName.split("\\.").last,
          ArcaneSchema(Seq(MergeKeyField)),
          BlobSourceWatermark.epoch
        )
        streamRunner <- Common.getTestApp(Duration.ofSeconds(60), unstableStreamContextLayer).fork
        _            <- streamRunner.runOrFail(Duration.ofSeconds(45))
        rows <- readTarget(
          unstableStreamContext.sink.targetTableFullName,
          "col0, col1, col2, col3, col4, col5, col6, col7, col8, col9, arcane_merge_key, createdon",
          Common.TargetDecoder
        )
      yield assertTrue(rows.size == 100) // no new rows added after stream has started
    },
    test("runs backfill from a JSON source - files contain nested array") {
      for
        _              <- TestSystem.putEnv("STREAMCONTEXT__BACKFILL", "true")
        _              <- ZIO.attempt(clearTarget(targetTableNameNested))
        backfillRunner <- Common.getTestApp(Duration.ofSeconds(60), nestedStreamContextLayer).fork
        _              <- backfillRunner.runOrFail(Duration.ofSeconds(45))
        rows <- readTarget(
          nestedStreamContext.sink.targetTableFullName,
          "col0, col1, col2, col3, col4, col5, col6, col7, col8, col9, nested_col_1, nested_col_2, arcane_merge_key, createdon",
          Common.TargetNestedDecoder
        )
      yield assertTrue(rows.size == 100) // col0 only have 100 unique values, thus we expect 100 rows total
    },
    test("runs stream correctly from a nested JSON source - file schema contains nested arrays") {
      for
        _ <- prepareWatermark(
          targetTableNameNested.split("\\.").last,
          ArcaneSchema(Seq(MergeKeyField)),
          BlobSourceWatermark.epoch
        )
        streamRunner <- Common.getTestApp(Duration.ofSeconds(60), nestedStreamContextLayer).fork
        _            <- streamRunner.runOrFail(Duration.ofSeconds(45))
        rows <- readTarget(
          nestedStreamContext.sink.targetTableFullName,
          "col0, col1, col2, col3, col4, col5, col6, col7, col8, col9, nested_col_1, nested_col_2, arcane_merge_key, createdon",
          Common.TargetNestedDecoder
        )
      yield assertTrue(rows.size == 100) // no new rows added after stream has started
    }
  ) @@ timeout(zio.Duration.fromSeconds(240)) @@ TestAspect.withLiveClock @@ TestAspect.sequential @@ TestAspect.before(
    liveSeed
  )
