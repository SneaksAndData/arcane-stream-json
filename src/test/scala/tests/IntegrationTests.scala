package com.sneaksanddata.arcane.stream_json
package tests

import models.UpsertBlobStreamContext
import models.app.StreamSpec
import tests.Common.{StreamContextLayer, avroSchemaString, nestedAvroSchemaString}

import zio.test.TestAspect.timeout
import zio.test.*
import zio.{Scope, Task, ULayer, ZIO, ZLayer}

import java.time.Duration

object IntegrationTests extends ZIOSpecDefault:
  val targetTableName      = "iceberg.test.stream_run"

  val stableSourceBucket   = "s3-blob-reader-json"
  val unstableSourceBucket = "s3-blob-reader-json-variable"

  val nestedSourceBucket = "s3-blob-reader-json-nested-array"
  var targetTableNameNested = "iceberg.test.stream_nested_run"


  private def getStreamContextStr(targetTable: String, sourceBucket: String, schema: String, jsonPointerExpr: String, jsonArrayPointers: String) =
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
       |  "lookBackInterval": 300,
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
       |    "targetTableName": "$targetTable"
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
       |      "namespace": "test",
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

  private val stableParsedSpec   = StreamSpec.fromString(getStreamContextStr(targetTableName, stableSourceBucket, avroSchemaString, "", "{}"))
  private val unstableParsedSpec = StreamSpec.fromString(getStreamContextStr(targetTableName, unstableSourceBucket, avroSchemaString, "", "{}"))
  private val nestedParsedSpec = StreamSpec.fromString(getStreamContextStr(targetTableNameNested, nestedSourceBucket, nestedAvroSchemaString, "/body", "{ \"/nested_array/value\": {} }"))

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

  private val stableStreamingStreamContextLayer: ULayer[UpsertBlobStreamContext] =
    ZLayer.succeed[UpsertBlobStreamContext](stableStreamingStreamContext)

  private val unstableStreamingStreamContextLayer: ULayer[UpsertBlobStreamContext] =
    ZLayer.succeed[UpsertBlobStreamContext](unstableStreamingStreamContext)

  private val nestedStreamingStreamContextLayer: ULayer[UpsertBlobStreamContext] =
    ZLayer.succeed[UpsertBlobStreamContext](nestedStreamingStreamContext)

  private val stableBackfillStreamContextLayer = ZLayer.succeed[UpsertBlobStreamContext](stableBackfillStreamContext)
  private val unstableBackfillStreamContextLayer =
    ZLayer.succeed[UpsertBlobStreamContext](unstableBackfillStreamContext)
  private val nestedBackfillStreamContextLayer =
    ZLayer.succeed[UpsertBlobStreamContext](nestedBackfillStreamContext)

  override def spec: Spec[TestEnvironment & Scope, Any] = suite("IntegrationTests")(
    test("runs backfill from a stable JSON source - file schema identical") {
      for
        _              <- ZIO.attempt(Fixtures.clearTarget(targetTableName))
        backfillRunner <- Common.buildTestApp(TimeLimitLifetimeService.layer, stableBackfillStreamContextLayer).fork
        _ <- backfillRunner.join.timeout(Duration.ofSeconds(15))
        _ <- Common.waitForData(
          stableBackfillStreamContext.targetTableFullName,
          "col0, col1, col2, col3, col4, col5, col6, col7, col8, col9, arcane_merge_key, createdon",
          Common.TargetDecoder,
          100 // col0 only have 100 unique values, thus we expect 100 rows total
        )
      yield assertTrue(true)
    },
    test("runs stream correctly from a stable JSON source - file schema identical") {
      for
        streamRunner <- Common.buildTestApp(TimeLimitLifetimeService.layer, stableStreamingStreamContextLayer).fork
        _ <- streamRunner.join.timeout(Duration.ofSeconds(15))
        rows <- Common.getData(
          stableStreamingStreamContext.targetTableFullName,
          "col0, col1, col2, col3, col4, col5, col6, col7, col8, col9, arcane_merge_key, createdon",
          Common.TargetDecoder
        )
      yield assertTrue(rows.size == 100) // no new rows added after stream has started
    },
    test("runs backfill from an unstable JSON source - file schema varies from file to file") {
      for
        _              <- ZIO.attempt(Fixtures.clearTarget(targetTableName))
        backfillRunner <- Common.buildTestApp(TimeLimitLifetimeService.layer, unstableBackfillStreamContextLayer).fork
        _ <- backfillRunner.join.timeout(Duration.ofSeconds(15))
        _ <- Common.waitForData(
          unstableBackfillStreamContext.targetTableFullName,
          "col0, col1, col2, col3, col4, col5, col6, col7, col8, col9, arcane_merge_key, createdon",
          Common.TargetDecoder,
          100 // col0 only have 100 unique values, thus we expect 100 rows total
        )
      yield assertTrue(true)
    },
    test("runs stream correctly from an unstable JSON source - file schema varies from file to file") {
      for
        streamRunner <- Common.buildTestApp(TimeLimitLifetimeService.layer, unstableStreamingStreamContextLayer).fork
        _ <- streamRunner.join.timeout(Duration.ofSeconds(15))
        rows <- Common.getData(
          unstableStreamingStreamContext.targetTableFullName,
          "col0, col1, col2, col3, col4, col5, col6, col7, col8, col9, arcane_merge_key, createdon",
          Common.TargetDecoder
        )
      yield assertTrue(rows.size == 100) // no new rows added after stream has started
    },
    test("runs backfill from a JSON source - files contain nested array") {
      for
        _              <- ZIO.attempt(Fixtures.clearTarget(targetTableNameNested))
        backfillRunner <- Common.buildTestApp(TimeLimitLifetimeService.layer, nestedBackfillStreamContextLayer).fork
        _ <- backfillRunner.join.timeout(Duration.ofSeconds(15))
        _ <- Common.waitForData(
          nestedBackfillStreamContext.targetTableFullName,
          "col0, col1, col2, col3, col4, col5, col6, col7, col8, col9, nested_col_1, nested_col_2, arcane_merge_key, createdon",
          Common.TargetNestedDecoder,
          100 // col0 only have 100 unique values, thus we expect 100 rows total
        )
      yield assertTrue(true)
    },
  ) @@ timeout(zio.Duration.fromSeconds(180)) @@ TestAspect.withLiveClock @@ TestAspect.sequential
