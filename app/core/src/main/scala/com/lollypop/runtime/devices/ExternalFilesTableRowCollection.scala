package com.lollypop.runtime.devices

import com.lollypop.language.dieFileNotFound
import com.lollypop.runtime.DatabaseManagementSystem.readExternalTable
import com.lollypop.runtime.DatabaseObjectConfig.ExternalTableConfig
import com.lollypop.runtime.DatabaseObjectRef.DatabaseObjectRefRealization
import com.lollypop.runtime.RuntimeFiles.RecursiveFileList
import com.lollypop.runtime.devices.ExternalFilesTableRowCollection.EOF
import com.lollypop.runtime.devices.RecordCollectionZoo.MapToRow
import com.lollypop.runtime.devices.errors.{FormatNotSpecified, UnsupportedFormatError}
import com.lollypop.runtime.{DatabaseObjectNS, DatabaseObjectRef, ROWID, Scope}
import com.lollypop.util.OptionHelper.OptionEnrichment
import com.lollypop.util.ResourceHelper._
import lollypop.io.DataFileConversion

import java.io.File
import scala.io.Source

/**
 * Represents a virtual table using external files as its source of data
 * @param config the [[ExternalTableConfig table configuration]]
 * @param host   the host [[RowCollection collection]]
 * @example {{{
 * create external table demo_stocks (
 *    symbol: String(5),
 *    exchange: String(6),
 *    lastSale: Double,
 *    transactionTime: Long
 * ) containing { location: './demos/stocks.csv', headers: false }
 * }}}
 */
class ExternalFilesTableRowCollection(val config: ExternalTableConfig, val host: RowCollection)
  extends HostedRowCollection with ReadOnlyRecordCollection[Row] {
  // get the config details
  private val rootFile: File = config.location.map(new File(_)) || dieFileNotFound(ns)
  private val parseText: String => Map[String, Any] = DataFileConversion.lookupConverter(columns, config, rootFile) match {
    case Some(converter) => converter.parse
    case None => config.format.map(s => throw new UnsupportedFormatError(s)) || (throw new FormatNotSpecified())
  }

  private val ensureData: ROWID => Unit = rootFile match {
    case d if d.isDirectory => ensureDataFromDirectory(d)
    case f => ensureDataFromFile(f)
  }

  // setup the raw device
  private var watermark: ROWID = 0
  host.setLength(newSize = watermark + 1)

  // ensure the data upto the watermark
  ensureData(watermark)

  override def apply(rowID: ROWID): Row = {
    ensureData(rowID)
    super.apply(rowID)
  }

  override def getLength: ROWID = {
    ensureData(EOF)
    super.getLength
  }

  override def hasNext(rowID: ROWID): Boolean = {
    ensureData(rowID)
    super.hasNext(rowID)
  }

  override def readField(rowID: ROWID, columnID: Int): Field = {
    ensureData(rowID)
    super.readField(rowID, columnID)
  }

  override def readFieldMetadata(rowID: ROWID, columnID: Int): FieldMetadata = {
    ensureData(rowID)
    super.readFieldMetadata(rowID, columnID)
  }

  override def readRowMetadata(rowID: ROWID): RowMetadata = {
    ensureData(rowID)
    super.readRowMetadata(rowID)
  }

  override def sizeInBytes: Long = {
    ensureData(EOF)
    super.sizeInBytes
  }

  private def ensureDataFromFile(file: File): ROWID => Unit = {
    var isClosed: Boolean = false
    val bs = Source.fromFile(file)
    val lines = bs.getLines()
    if (lines.hasNext & config.headers.contains(true)) lines.next()

    (rowID: ROWID) => {
      if (!isClosed) {
        // process rows from the file until we reach the desired rowID
        while (watermark <= rowID & lines.hasNext) {
          val line = lines.next()
          val mapping = parseText(line)
          host.update(watermark, mapping.toRow(watermark)(host))
          watermark += 1
        }

        // if no more lines, close the stream
        if (!lines.hasNext) {
          isClosed = true
          bs.close()
        }
      }
    }
  }

  private def ensureDataFromDirectory(directory: File): ROWID => Unit = {
    val files: Iterator[File] = directory.streamFilesRecursively.iterator
    assert(files.hasNext, s"No files found in '${directory.getCanonicalPath}'")

    (rowID: ROWID) =>
      while (watermark <= rowID && files.hasNext) {
        val file = files.next()
        Source.fromFile(file).use { bs =>
          val lines = bs.getLines()
          if (lines.hasNext && config.headers.contains(true)) lines.next()
          lines foreach { line =>
            val mapping = parseText(line)
            host.update(watermark, mapping.toRow(watermark)(host))
            watermark += 1
          }
        }
      }
  }

}

/**
 * External Files Table Device Companion
 */
object ExternalFilesTableRowCollection {
  private val EOF = Long.MaxValue

  /**
   * Loads an External Table File
   * @param ns the [[DatabaseObjectNS table namespace]]
   * @return the [[RowCollection external table]]
   */
  def apply(ns: DatabaseObjectNS): RowCollection = readExternalTable(ns)

  /**
   * Loads an External Table File
   * @param ref   the [[DatabaseObjectRef table reference]]
   * @param scope the implicit [[Scope scope]]
   * @return the [[RowCollection external table]]
   */
  def apply(ref: DatabaseObjectRef)(implicit scope: Scope): RowCollection = readExternalTable(ref.toNS)

}