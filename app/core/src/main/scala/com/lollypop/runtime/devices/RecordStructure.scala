package com.lollypop.runtime.devices

import com.lollypop.die
import com.lollypop.language._
import com.lollypop.runtime.{ROWID, _}

import java.nio.ByteBuffer
import java.nio.ByteBuffer.allocate

/**
 * Represents a physical record layout/structure
 */
trait RecordStructure {

  /**
   * @return the collection of [[TableColumn columns]]
   */
  def columns: Seq[TableColumn]

  /**
   * @return the column offset collection
   */
  def columnOffsets: List[Int] = {
    case class Accumulator(agg: Int = 0, var last: Int = FieldMetadata.BYTES_LENGTH, var list: List[Int] = Nil)
    columns.map(getMaxPhysicalSize).foldLeft(Accumulator()) { (acc, maxLength) =>
      val index = acc.agg + acc.last
      acc.last = maxLength + index
      acc.list = index :: acc.list
      acc
    }.list.reverse
  }

  /**
   * Retrieves a column by ID
   * @param columnID the column ID
   * @return the [[TableColumn column]]
   */
  def getColumnByID(columnID: Int): TableColumn = columns(columnID)

  /**
   * Retrieves a column ID
   * @param name the column name
   * @return the option of a column index
   */
  def getColumnIdByName(name: String): Option[Int] = columns.indexWhere(_.name == name) match {
    case -1 => None
    case index => Some(index)
  }

  /**
   * Retrieves a column ID
   * @param name the column name
   * @return the column index
   */
  def getColumnIdByNameOrDie(name: String): Int = getColumnIdByName(name) || dieNoSuchColumn(name)

  /**
   * Retrieves a column by name
   * @param name the column name
   * @return the [[TableColumn column]]
   */
  def getColumnByName(name: String): TableColumn = {
    nameToColumnMap().getOrElse(name, dieNoSuchColumn(name))
  }

  /**
   * @return a name to column mapping
   */
  private def nameToColumnMap: () => Map[String, TableColumn] = {
    val mapping = Map(columns.map(c => c.name -> c): _*)
    () => mapping
  }

  /**
   * @return the record length in bytes
   */
  def recordSize: Int = RowMetadata.BYTES_LENGTH + columns.map(getMaxPhysicalSize).sum

  protected def getMaxPhysicalSize(column: TableColumn): Int = {
    column.`type`.maxPhysicalSize
  }

  def toFieldBuffers(rowID: ROWID, fields: Seq[Field]): Seq[ByteBuffer] = {
    columns zip fields map {
      case (column, _) if column.`type`.isAutoIncrement => column.`type`.encodeFull(rowID)
      case (column, field) if column.isExternal => encodeFull(column, field)
      case (column, field) =>
        try {
          val fieldBuf = encodeFull(column, field)
          val maxPhysicalSize = getMaxPhysicalSize(column)
          if (fieldBuf.limit() <= maxPhysicalSize) allocate(maxPhysicalSize).put(fieldBuf).flipMe()
          else die(s"Column overflow `${column.name}`: ${fieldBuf.limit()} > $maxPhysicalSize")
        } catch {
          case e: Throwable =>
            die(s"${e.getMessage} for field '${field.name}' (defined as: ${column.toSQL})", e)
        }
    }
  }

  private def encodeFull(column: TableColumn, field: Field): ByteBuffer = {
    column.`type`.encodeFull(field.value ?? column.defaultValue.flatMap(v => Option(v.evaluate()._3)))
  }

  def toFields(buf: ByteBuffer): Seq[Field] = {
    columns.zipWithIndex map { case (column, index) =>
      buf.position(columnOffsets(index))
      column.name -> column.`type`.decodeFull(buf)
    } map { case (name, (fmd, value_?)) => Field(name, fmd, value_?) }
  }

  def toRowBuffer(fields: Seq[ByteBuffer]): ByteBuffer = {
    val buf = allocate(recordSize)
    buf.putRowMetadata(RowMetadata())
    fields.zip(columns) zip columnOffsets foreach { case ((fieldBuf, column), offset) =>
      putFieldBuffer(column, buf, fieldBuf, offset)
    }
    buf.flipMe()
  }

  def putFieldBuffer(column: TableColumn, buf: ByteBuffer, fieldBuf: ByteBuffer, offset: Int): ByteBuffer = {
    val required = offset + fieldBuf.limit()
    val available = buf.capacity()
    assert(required <= available, die(s"Buffer overflow for column '${column.name}': required ($required) > available ($available)"))
    buf.position(offset)
    buf.put(fieldBuf)
  }

}

/**
 * RecordStructure Companion
 */
object RecordStructure {

  def apply(tableColumns: Seq[TableColumn]): RecordStructure = new RecordStructure {
    override val columns: Seq[TableColumn] = tableColumns
  }

}