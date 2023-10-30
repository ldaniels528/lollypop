package lollypop.io

import com.lollypop.runtime.DatabaseObjectConfig.ExternalTableConfig
import com.lollypop.runtime.RuntimeFiles.RecursiveFileList
import com.lollypop.runtime.devices.TableColumn
import com.lollypop.util.OptionHelper.OptionEnrichment

import java.io.File

/**
 * Represents a Data File Conversion
 */
trait DataFileConversion {

  def apply(columns: Seq[TableColumn], config: ExternalTableConfig, file: File): DataFileConverter

  def understands(file: File): Boolean

  def understandsFormat(format: String): Boolean

}

/**
 * Data File Format Singleton
 */
object DataFileConversion {
  private var conversions: List[DataFileConversion] = Nil

  def lookupConverter(columns: Seq[TableColumn], config: ExternalTableConfig, file: File): Option[DataFileConverter] = {
    val format = config.format ?? guessFileFormat(file)
    conversions.collectFirst { case cv if cv.understands(file) || format.exists(cv.understandsFormat) =>
      cv(columns, config, file)
    }
  }

  def registerConversion(conversion: DataFileConversion): this.type = {
    conversions = conversion :: conversions
    this
  }

  private def guessFileFormat(file: File): Option[String] = {
    val extensions = Seq(".csv", ".json", ".tsv", ".xls")
    file match {
      case d if d.isDirectory =>
        val filename_? = d.streamFilesRecursively.headOption.map(_.getName.toLowerCase())
        extensions.find(ext => filename_?.exists(_.endsWith(ext))).map(_.drop(1))
      case f if f.isFile =>
        val filename = f.getName.toLowerCase()
        extensions.find(filename.endsWith).map(_.drop(1))
      case _ => None
    }
  }

  // pre-register CSV and JSON
  registerConversion(CSVConversion)
  registerConversion(JSONConversion)

}