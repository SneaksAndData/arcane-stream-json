package com.sneaksanddata.arcane.stream_json
package models.app

import com.sneaksanddata.arcane.framework.models.settings.sources.blob.DefaultJsonBlobSourceSettings
import com.sneaksanddata.arcane.framework.models.settings.sources.modification.DefaultOverrideDataRowModificationSettings
import com.sneaksanddata.arcane.framework.models.settings.sources.{
  DefaultOverrideSourceBufferingSettings,
  OverrideStreamSourceSettings
}
import upickle.ReadWriter

/** A partial override of `JsonStreamSourceSettings` provided by the Arcane operator via the environment variable.
  */
case class OverrideJsonStreamSourceSettings(
    override val buffering: Option[DefaultOverrideSourceBufferingSettings] = None,
    override val configuration: Option[DefaultJsonBlobSourceSettings] = None,
    override val modifications: Option[DefaultOverrideDataRowModificationSettings] = None
) extends OverrideStreamSourceSettings derives ReadWriter:
  override type SourceSettingsOverrideType = DefaultJsonBlobSourceSettings
