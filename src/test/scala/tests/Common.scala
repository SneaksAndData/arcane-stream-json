package com.sneaksanddata.arcane.stream_json
package tests

import main.{appLayer, blobSourceLayer, s3ReaderLayer}
import models.app.JsonPluginStreamContext

import com.sneaksanddata.arcane.framework.plugins.LayerAssemblies
import com.sneaksanddata.arcane.framework.plugins.jsons3.Services
import com.sneaksanddata.arcane.framework.services.app.{GenericStreamRunnerService, StreamGraphResolver}
import com.sneaksanddata.arcane.framework.testkit.appbuilder.TestAppBuilder.buildTestApp
import zio.metrics.connectors.MetricsConfig
import zio.metrics.connectors.datadog.DatadogPublisherConfig
import zio.metrics.connectors.statsd.DatagramSocketConfig
import zio.{ZIO, ZLayer}

import java.sql.ResultSet
import java.time.Duration

/** Common utilities for tests.
  */
object Common:

  /** Builds the test application from the provided layers.
    * @param streamContextLayer
    *   The stream context layer.
    * @return
    *   The test application.
    */
  def getTestApp(
      runDuration: Duration,
      streamContextLayer: ZLayer[
        Any,
        Nothing,
        JsonPluginStreamContext & DatagramSocketConfig & MetricsConfig & DatadogPublisherConfig
      ]
  ): ZIO[Any, Throwable, Unit] =
    buildTestApp(
      appLayer,
      streamContextLayer,
      s3ReaderLayer
    )(
      blobSourceLayer,
      Services.sourceLayer,
      LayerAssemblies.frameworkPipelineServicesLayer,
      LayerAssemblies.frameworkStagingServicesLayer,
      GenericStreamRunnerService.layer,
      StreamGraphResolver.composedLayer
    )

  val TargetDecoder: ResultSet => (Long, String, Long, String, Long, String, Long, String, Long, String, String, Long) =
    (rs: ResultSet) =>
      (
        rs.getLong(1),
        rs.getString(2),
        rs.getLong(3),
        rs.getString(4),
        rs.getLong(5),
        rs.getString(6),
        rs.getLong(7),
        rs.getString(8),
        rs.getLong(9),
        rs.getString(10),
        rs.getString(11),
        rs.getLong(12)
      )

  val TargetNestedDecoder: ResultSet => (
      Long,
      String,
      Long,
      String,
      Long,
      String,
      Long,
      String,
      Long,
      String,
      String,
      Long,
      String,
      Long
  ) =
    (rs: ResultSet) =>
      (
        rs.getLong(1),
        rs.getString(2),
        rs.getLong(3),
        rs.getString(4),
        rs.getLong(5),
        rs.getString(6),
        rs.getLong(7),
        rs.getString(8),
        rs.getLong(9),
        rs.getString(10),
        rs.getString(11),
        rs.getLong(12),
        rs.getString(13),
        rs.getLong(14)
      )

  val avroSchemaString =
    """{ \"name\": \"GeneratedAvroSchemaTest\", \"namespace\": \"com.group.GeneratedAvroSchemaTest\", \"doc\": \"Unit test data schema\", \"type\": \"record\", \"fields\": [ { \"name\": \"col0\", \"type\": [ \"null\", \"int\" ], \"default\": null }, { \"name\": \"col1\", \"type\": [ \"null\", \"string\" ], \"default\": null }, { \"name\": \"col2\", \"type\": [ \"null\", \"int\" ], \"default\": null }, { \"name\": \"col3\", \"type\": [ \"null\", \"string\" ], \"default\": null }, { \"name\": \"col4\", \"type\": [ \"null\", \"int\" ], \"default\": null }, { \"name\": \"col5\", \"type\": [ \"null\", \"string\" ], \"default\": null }, { \"name\": \"col6\", \"type\": [ \"null\", \"int\" ], \"default\": null }, { \"name\": \"col7\", \"type\": [ \"null\", \"string\" ], \"default\": null }, { \"name\": \"col8\", \"type\": [ \"null\", \"int\" ], \"default\": null }, { \"name\": \"col9\", \"type\": [ \"null\", \"string\" ], \"default\": null } ] }"""
  val nestedAvroSchemaString =
    """{ \"name\": \"BlobListingJsonSource\", \"namespace\": \"com.sneaksanddata.arcane.BlobListingJsonSource\", \"doc\": \"Avro Schema with nested fields for BlobListingJsonSource tests\", \"type\": \"record\", \"fields\": [ { \"name\": \"col0\", \"type\": [ \"null\", \"int\" ], \"default\": null }, { \"name\": \"col1\", \"type\": [ \"null\", \"string\" ], \"default\": null }, { \"name\": \"col2\", \"type\": [ \"null\", \"int\" ], \"default\": null }, { \"name\": \"col3\", \"type\": [ \"null\", \"string\" ], \"default\": null }, { \"name\": \"col4\", \"type\": [ \"null\", \"int\" ], \"default\": null }, { \"name\": \"col5\", \"type\": [ \"null\", \"string\" ], \"default\": null }, { \"name\": \"col6\", \"type\": [ \"null\", \"int\" ], \"default\": null }, { \"name\": \"col7\", \"type\": [ \"null\", \"string\" ], \"default\": null }, { \"name\": \"col8\", \"type\": [ \"null\", \"int\" ], \"default\": null }, { \"name\": \"col9\", \"type\": [ \"null\", \"string\" ], \"default\": null }, { \"name\": \"nested_col_1\", \"type\": [ \"null\", \"string\" ], \"default\": null }, { \"name\": \"nested_col_2\", \"type\": [ \"null\", \"int\" ], \"default\": null } ] }"""
