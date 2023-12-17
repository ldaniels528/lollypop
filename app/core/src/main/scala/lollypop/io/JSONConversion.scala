package lollypop.io

import com.lollypop.language.dieExpectedJSONObject
import com.lollypop.runtime.DatabaseObjectConfig.ExternalTableConfig
import com.lollypop.runtime._
import com.lollypop.runtime.devices.{QMap, TableColumn}

import java.io.File

object JSONConversion extends DataFileConversion {

  def apply(columns: Seq[TableColumn], config: ExternalTableConfig, file: File): DataFileConverter = {
    new JSONConverter(config)
  }

  override def understands(file: File): Boolean = file.getName.toLowerCase.endsWith(".json")

  override def understandsFormat(format: String): Boolean = format.toLowerCase == "json"

  ////////////////////////////////////////////////////////////////////////////////////////////////////
  //    CONVERTERS
  ////////////////////////////////////////////////////////////////////////////////////////////////////

  private class JSONConverter(config: ExternalTableConfig) extends DataFileConverter {
    override def parse(line: String): Map[String, Any] = {
      import spray.json._
      line.parseJson.unwrapJSON match {
        case m: QMap[String, Any] => m.toMap
        case other => dieExpectedJSONObject(other)
      }
    }
  }

}