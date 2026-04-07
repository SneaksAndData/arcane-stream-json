package com.sneaksanddata.arcane.stream_json
package tests

import tests.Common.{avroSchemaString, nestedAvroSchemaString}

import com.sneaksanddata.arcane.framework.models.schemas.{ArcaneSchema, MergeKeyField}
import com.sneaksanddata.arcane.framework.services.blobsource.versioning.BlobSourceWatermark
import com.sneaksanddata.arcane.framework.testkit.setups.FrameworkTestSetup.prepareWatermark
import com.sneaksanddata.arcane.framework.testkit.verifications.FrameworkVerificationUtilities.{clearTarget, readTarget}
import com.sneaksanddata.arcane.framework.testkit.zioutils.ZKit.runOrFail
import com.sneaksanddata.arcane.stream_json.models.app.JsonPluginStreamContext
import zio.metrics.connectors.MetricsConfig
import zio.metrics.connectors.datadog.DatadogPublisherConfig
import zio.metrics.connectors.statsd.DatagramSocketConfig
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
       |
       |{
       |  "backfillJobTemplateRef": {
       |    "apiGroup": "streaming.sneaksanddata.com",
       |    "kind": "StreamingJobTemplate",
       |    "name": "arcane-stream-json-large-job"
       |  },
       |  "groupingIntervalSeconds": 1,
       |  "jobTemplateRef": {
       |    "apiGroup": "streaming.sneaksanddata.com",
       |    "kind": "StreamingJobTemplate",
       |    "name": "arcane-stream-json-standard-job"
       |  },
       |  "tableProperties": {
       |    "partitionExpressions": [],
       |    "format": "PARQUET",
       |    "sortedBy": [],
       |    "parquetBloomFilterColumns": []
       |  },
       |  "rowsPerGroup": 1000,
       |  "sinkSettings": {
       |    "optimizeSettings": {
       |      "batchThreshold": 60,
       |      "fileSizeThreshold": "512MB"
       |    },
       |    "orphanFilesExpirationSettings": {
       |      "batchThreshold": 60,
       |      "retentionThreshold": "6h"
       |    },
       |    "snapshotExpirationSettings": {
       |      "batchThreshold": 60,
       |      "retentionThreshold": "6h"
       |    },
       |    "analyzeSettings": {
       |      "batchThreshold": 60,
       |      "includedColumns": []
       |    },
       |    "targetTableName": "$targetTable",
       |    "sinkCatalogSettings": {
       |      "namespace": "test",
       |      "warehouse": "demo",
       |      "catalogUri": "http://localhost:20001/catalog"
       |    }
       |  },
       |  "sourceSettings": {
       |    "changeCaptureIntervalSeconds": 5,
       |    "baseLocation": "s3a://$sourceBucket",
       |    "tempPath": "/tmp",
       |    "primaryKeys": ["col0"],
       |    "s3": {
       |      "usePathStyle": true,
       |      "region": "us-east-1",
       |      "endpoint": "http://localhost:9000",
       |      "maxResultsPerPage": 150,
       |      "retryMaxAttempts": 5,
       |      "retryBaseDelay": 0.1,
       |      "retryMaxDelay": 1
       |    },
       |    "avroSchemaString": "$schema",
       |    "jsonPointerExpression": "$jsonPointerExpr",
       |    "jsonArrayPointers": $jsonArrayPointers
       |  },
       |  "stagingDataSettings": {
       |    "catalog": {
       |      "catalogName": "iceberg",
       |      "catalogUri": "http://localhost:20001/catalog",
       |      "schemaName": "test",
       |      "warehouse": "demo"
       |    },
       |    "tableNamePrefix": "staging_${targetTable.replace(".", "_")}",
       |    "maxRowsPerFile": 10000
       |  },
       |  "fieldSelectionRule": {
       |    "ruleType": "all",
       |    "fields": []
       |  },
       |  "backfillBehavior": "overwrite",
       |  "backfillStartDate": "1735731264"
       |}""".stripMargin

  private val stableStreamContext = JsonPluginStreamContext(getStreamContextStr(targetTableName, stableSourceBucket, avroSchemaString, "", "{}"))
  private val stableStreamContextLayer = ZLayer.succeed(stableStreamContext)
  
  private val unstableStreamContext = JsonPluginStreamContext(getStreamContextStr(targetTableName, unstableSourceBucket, avroSchemaString, "", "{}"))
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
        _ <- TestSystem.putEnv("STREAMCONTEXT__BACKFILL", "true")
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
        _ <- TestSystem.putEnv("STREAMCONTEXT__BACKFILL", "true")
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
        _ <- TestSystem.putEnv("STREAMCONTEXT__BACKFILL", "true")
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
        _            <- streamRunner.join.timeout(Duration.ofSeconds(45))
        rows <- readTarget(
          nestedStreamContext.sink.targetTableFullName,
          "col0, col1, col2, col3, col4, col5, col6, col7, col8, col9, nested_col_1, nested_col_2, arcane_merge_key, createdon",
          Common.TargetNestedDecoder
        )
      yield assertTrue(rows.size == 100) // no new rows added after stream has started
    }
  ) @@ timeout(zio.Duration.fromSeconds(240)) @@ TestAspect.withLiveClock @@ TestAspect.sequential
