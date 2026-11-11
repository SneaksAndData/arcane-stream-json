package com.sneaksanddata.arcane.stream_json
package models.app

import com.sneaksanddata.arcane.framework.models.settings.Mergeable
import com.sneaksanddata.arcane.framework.models.settings.sources.blob.DefaultJsonBlobSourceSettings
import com.sneaksanddata.arcane.framework.models.settings.sources.modification.DefaultDataRowModificationSettings
import com.sneaksanddata.arcane.framework.models.settings.sources.{DefaultSourceBufferingSettings, StreamSourceSettings}
import upickle.ReadWriter

case class JsonStreamSourceSettings(
    override val buffering: DefaultSourceBufferingSettings,
    override val modifications: DefaultDataRowModificationSettings,
    override val configuration: DefaultJsonBlobSourceSettings
) extends StreamSourceSettings,
      Mergeable derives ReadWriter:
  override type SourceSettingsType = DefaultJsonBlobSourceSettings

  override type MergeableFrom = OverrideJsonStreamSourceSettings
  override type MergeResult   = JsonStreamSourceSettings

  override def merge(overrides: Option[MergeableFrom]): MergeResult =
    JsonStreamSourceSettings(
      buffering = this.buffering.merge(overrides.flatMap(_.buffering)),
      modifications = this.modifications.merge(overrides.flatMap(_.modifications)),
      configuration
    )
