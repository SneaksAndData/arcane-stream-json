package com.sneaksanddata.arcane.stream_json
package tests

import models.UpsertBlobStreamContext
import models.app.StreamSpec
import tests.Common.{avroSchemaString, nestedAvroSchemaString}

import com.sneaksanddata.arcane.framework.models.schemas.{ArcaneSchema, MergeKeyField}
import com.sneaksanddata.arcane.framework.services.blobsource.versioning.BlobSourceWatermark
import com.sneaksanddata.arcane.framework.testkit.setups.FrameworkTestSetup.prepareWatermark
import com.sneaksanddata.arcane.framework.testkit.verifications.FrameworkVerificationUtilities.{clearTarget, readTarget}
import com.sneaksanddata.arcane.framework.testkit.zioutils.ZKit.runOrFail
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

  private val stableParsedSpec =
    StreamSpec.fromString(getStreamContextStr(targetTableName, stableSourceBucket, avroSchemaString, "", "{}"))
  private val unstableParsedSpec =
    StreamSpec.fromString(getStreamContextStr(targetTableName, unstableSourceBucket, avroSchemaString, "", "{}"))
  private val nestedParsedSpec = StreamSpec.fromString(
    getStreamContextStr(
      targetTableNameNested,
      nestedSourceBucket,
      nestedAvroSchemaString,
      "/body",
      "{ \"/nested_array/value\": {} }"
    )
  )

  private val stableStreamingStreamContext = new UpsertBlobStreamContext(stableParsedSpec):
    override val IsBackfilling: Boolean = false

  private val stableBackfillStreamContext = new UpsertBlobStreamContext(stableParsedSpec):
    override val IsBackfilling: Boolean = true

  private val unstableStreamingStreamContext = new UpsertBlobStreamContext(unstableParsedSpec):
    override val IsBackfilling: Boolean = false

  private val unstableBackfillStreamContext = new UpsertBlobStreamContext(unstableParsedSpec):
    override val IsBackfilling: Boolean = true

  private val nestedStreamingStreamContext = new UpsertBlobStreamContext(nestedParsedSpec):
    override val IsBackfilling: Boolean = false

  private val nestedBackfillStreamContext = new UpsertBlobStreamContext(nestedParsedSpec):
    override val IsBackfilling: Boolean = true

  private val stableStreamingStreamContextLayer =
    ZLayer.succeed[UpsertBlobStreamContext](stableStreamingStreamContext)
      ++ ZLayer.succeed(DatagramSocketConfig("/var/run/datadog/dsd.socket"))
      ++ ZLayer.succeed(MetricsConfig(Duration.ofMillis(100)))
      ++ ZLayer.succeed(DatadogPublisherConfig())

  private val unstableStreamingStreamContextLayer =
    ZLayer.succeed[UpsertBlobStreamContext](unstableStreamingStreamContext)
      ++ ZLayer.succeed(DatagramSocketConfig("/var/run/datadog/dsd.socket"))
      ++ ZLayer.succeed(MetricsConfig(Duration.ofMillis(100)))
      ++ ZLayer.succeed(DatadogPublisherConfig())

  private val nestedStreamingStreamContextLayer =
    ZLayer.succeed[UpsertBlobStreamContext](nestedStreamingStreamContext)
      ++ ZLayer.succeed(DatagramSocketConfig("/var/run/datadog/dsd.socket"))
      ++ ZLayer.succeed(MetricsConfig(Duration.ofMillis(100)))
      ++ ZLayer.succeed(DatadogPublisherConfig())

  private val stableBackfillStreamContextLayer = ZLayer.succeed[UpsertBlobStreamContext](stableBackfillStreamContext)
    ++ ZLayer.succeed(DatagramSocketConfig("/var/run/datadog/dsd.socket"))
    ++ ZLayer.succeed(MetricsConfig(Duration.ofMillis(100)))
    ++ ZLayer.succeed(DatadogPublisherConfig())
  private val unstableBackfillStreamContextLayer =
    ZLayer.succeed[UpsertBlobStreamContext](unstableBackfillStreamContext)
      ++ ZLayer.succeed(DatagramSocketConfig("/var/run/datadog/dsd.socket"))
      ++ ZLayer.succeed(MetricsConfig(Duration.ofMillis(100)))
      ++ ZLayer.succeed(DatadogPublisherConfig())
  private val nestedBackfillStreamContextLayer =
    ZLayer.succeed[UpsertBlobStreamContext](nestedBackfillStreamContext)
      ++ ZLayer.succeed(DatagramSocketConfig("/var/run/datadog/dsd.socket"))
      ++ ZLayer.succeed(MetricsConfig(Duration.ofMillis(100)))
      ++ ZLayer.succeed(DatadogPublisherConfig())

  override def spec: Spec[TestEnvironment & Scope, Any] = suite("IntegrationTests")(
    test("runs backfill from a stable JSON source - file schema identical") {
      for
        _              <- ZIO.attempt(clearTarget(targetTableName))
        backfillRunner <- Common.getTestApp(Duration.ofSeconds(60), stableBackfillStreamContextLayer).fork
        _              <- backfillRunner.runOrFail(Duration.ofSeconds(45))
        target <- readTarget(
          stableBackfillStreamContext.targetTableFullName,
          "col0, col1, col2, col3, col4, col5, col6, col7, col8, col9, arcane_merge_key, createdon",
          Common.TargetDecoder
        )
      yield assertTrue(target.size == 100) // col0 only have 100 unique values, thus we expect 100 rows total
    },
    test("runs stream correctly from a stable JSON source - file schema identical") {
      for
        streamRunner <- Common.getTestApp(Duration.ofSeconds(60), stableStreamingStreamContextLayer).fork
        _            <- streamRunner.runOrFail(Duration.ofSeconds(45))
        rows <- readTarget(
          stableStreamingStreamContext.targetTableFullName,
          "col0, col1, col2, col3, col4, col5, col6, col7, col8, col9, arcane_merge_key, createdon",
          Common.TargetDecoder
        )
      yield assertTrue(rows.size == 100) // no new rows added after stream has started
    },
    test("runs backfill from an unstable JSON source - file schema varies from file to file") {
      for
        _              <- ZIO.attempt(clearTarget(targetTableName))
        backfillRunner <- Common.getTestApp(Duration.ofSeconds(60), unstableBackfillStreamContextLayer).fork
        _              <- backfillRunner.runOrFail(Duration.ofSeconds(45))
        rows <- readTarget(
          unstableBackfillStreamContext.targetTableFullName,
          "col0, col1, col2, col3, col4, col5, col6, col7, col8, col9, arcane_merge_key, createdon",
          Common.TargetDecoder
        )
      yield assertTrue(rows.size == 100) // col0 only have 100 unique values, thus we expect 100 rows total
    },
    test("runs stream correctly from an unstable JSON source - file schema varies from file to file") {
      for
        _            <- prepareWatermark(targetTableName, ArcaneSchema(Seq(MergeKeyField)), BlobSourceWatermark.epoch)
        streamRunner <- Common.getTestApp(Duration.ofSeconds(60), unstableStreamingStreamContextLayer).fork
        _            <- streamRunner.runOrFail(Duration.ofSeconds(45))
        rows <- readTarget(
          unstableStreamingStreamContext.targetTableFullName,
          "col0, col1, col2, col3, col4, col5, col6, col7, col8, col9, arcane_merge_key, createdon",
          Common.TargetDecoder
        )
      yield assertTrue(rows.size == 100) // no new rows added after stream has started
    },
    test("runs backfill from a JSON source - files contain nested array") {
      for
        _              <- ZIO.attempt(clearTarget(targetTableNameNested))
        backfillRunner <- Common.getTestApp(Duration.ofSeconds(60), nestedBackfillStreamContextLayer).fork
        _              <- backfillRunner.runOrFail(Duration.ofSeconds(45))
        rows <- readTarget(
          nestedBackfillStreamContext.targetTableFullName,
          "col0, col1, col2, col3, col4, col5, col6, col7, col8, col9, nested_col_1, nested_col_2, arcane_merge_key, createdon",
          Common.TargetNestedDecoder
        )
      yield assertTrue(rows.size == 100) // col0 only have 100 unique values, thus we expect 100 rows total
    },
    test("runs stream correctly from a nested JSON source - file schema contains nested arrays") {
      for
        _ <- prepareWatermark(targetTableNameNested, ArcaneSchema(Seq(MergeKeyField)), BlobSourceWatermark.epoch)
        streamRunner <- Common.getTestApp(Duration.ofSeconds(60), nestedStreamingStreamContextLayer).fork
        _            <- streamRunner.join.timeout(Duration.ofSeconds(45))
        rows <- readTarget(
          nestedStreamingStreamContext.targetTableFullName,
          "col0, col1, col2, col3, col4, col5, col6, col7, col8, col9, nested_col_1, nested_col_2, arcane_merge_key, createdon",
          Common.TargetNestedDecoder
        )
      yield assertTrue(rows.size == 100) // no new rows added after stream has started
    }
  ) @@ timeout(zio.Duration.fromSeconds(240)) @@ TestAspect.withLiveClock @@ TestAspect.sequential
