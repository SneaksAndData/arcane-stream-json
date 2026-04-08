package com.sneaksanddata.arcane.stream_json
package models.app

import com.sneaksanddata.arcane.framework.models.settings.DefaultFieldSelectionRuleSettings
import com.sneaksanddata.arcane.framework.models.settings.sources.{DefaultSourceBufferingSettings, StreamSourceSettings}
import com.sneaksanddata.arcane.framework.models.settings.sources.blob.DefaultJsonBlobSourceSettings
import upickle.ReadWriter

case class JsonStreamSourceSettings(
    override val buffering: DefaultSourceBufferingSettings,
    override val fieldSelectionRule: DefaultFieldSelectionRuleSettings,
    override val configuration: DefaultJsonBlobSourceSettings
) extends StreamSourceSettings derives ReadWriter:
  override type SourceSettingsType = DefaultJsonBlobSourceSettings
