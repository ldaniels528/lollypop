package lollypop.io

import com.lollypop.language._
import com.lollypop.runtime.DatabaseObjectConfig.ExternalTableConfig
import com.lollypop.runtime.RuntimeFiles.RecursiveFileList
import com.lollypop.runtime.devices.TableColumn

import java.io.File

object CSVFileConversion extends DataFileConversion {
  private val extensions = Seq(".csv", ".psv", ".tsv")

  def apply(columns: Seq[TableColumn], config: ExternalTableConfig, file: File): DataFileConverter = {
    new CSVConverter(columns, config, file)
  }

  override def understands(file: File): Boolean = extensions.exists(file.getName.toLowerCase.endsWith)

  override def understandsFormat(format: String): Boolean = extensions.contains("." + format.toLowerCase)

  ////////////////////////////////////////////////////////////////////////////////////////////////////
  //    CONVERTERS
  ////////////////////////////////////////////////////////////////////////////////////////////////////

  private class CSVConverter(columns: Seq[TableColumn], config: ExternalTableConfig, file: File) extends DataFileConverter {
    private val delimiter: Char = determineDelimiter

    override def parse(line: String): Map[String, Any] = {
      val values = line.delimitedSplit(delimiter)
      val pairs = columns.map(_.name) zip values flatMap {
        case (_, s) if s.isEmpty | config.nullValues.contains(s) => None
        case x => Some(x)
      }
      Map(pairs: _*)
    }

    private def determineDelimiter: Char = {
      config.fieldDelimiter.flatMap(_.headOption) ?? getDelimiterByFormat(config.format) ?? getDelimiterByFile(file) || ','
    }

    private def getDelimiterByFormat(format: Option[String]): Option[Char] = format map {
      case "csv" => ','
      case "psv" => '|'
      case "tsv" => '\t'
      case _ => ','
    }

    private def getDelimiterByFile(file: File): Option[Char] = {
      val format = file match {
        case d if d.isDirectory =>
          val filename_? = d.streamFilesRecursively.headOption.map(_.getName.toLowerCase())
          extensions.find(ext => filename_?.exists(_.endsWith(ext))).map(_.drop(1))
        case f if f.isFile =>
          val filename = f.getName.toLowerCase()
          extensions.find(filename.endsWith).map(_.drop(1))
        case _ => None
      }
      getDelimiterByFormat(format)
    }

  }

}



