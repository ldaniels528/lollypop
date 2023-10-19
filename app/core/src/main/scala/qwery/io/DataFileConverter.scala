package qwery.io

/**
 * Represents a Data File Converter
 */
trait DataFileConverter {

  def parse(line: String): Map[String, Any]

}
