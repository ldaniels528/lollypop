package com.lollypop.runtime.datatypes

import com.lollypop.implicits.MagicImplicits
import com.lollypop.language.models.{ColumnType, TableModel}
import com.lollypop.language.{ColumnTypeParser, HelpDoc, SQLCompiler, TokenStream, dieUnsupportedConversion}
import com.lollypop.runtime.datatypes.TableType.headerSize
import com.lollypop.runtime.conversions.ExpressiveTypeConversion
import com.lollypop.runtime.devices.RecordCollectionZoo.MapToRow
import com.lollypop.runtime.devices.RowCollectionZoo._
import com.lollypop.runtime.devices.TableColumn.implicits.{SQLToColumnConversion, TableColumnToSQLColumnConversion}
import com.lollypop.runtime.devices._
import com.lollypop.runtime.{INT_BYTES, ROWID, Scope}
import com.lollypop.util.ByteBufferHelper.DataTypeBuffer
import com.lollypop.util.JSONSupport.JsValueConversion
import com.lollypop.util.JVMSupport.NormalizeAny
import com.lollypop.util.OptionHelper.OptionEnrichment
import lollypop.io.Decoder
import spray.json.JsValue

import java.nio.ByteBuffer
import java.nio.ByteBuffer.allocate

/**
 * Represents a Table type
 * @param columns      the table/collection columns
 * @param capacity     the optional initial capacity
 * @param growthFactor the table growth factor
 * @param isPointer    indicates whether the table is a pointer
 * @param partitions   the optional collection of partition column names
 * @example
 * {{{
 *     create table [if not exists] Cars(
 *         Name: String,
 *         Miles_per_Gallon: Int,
 *         Cylinders: Int,
 *         Displacement: Int,
 *         Horsepower: Int,
 *         Weight_in_lbs: Int,
 *         Acceleration: Decimal,
 *         Year: Date,
 *         Origin: Char(1))
 * }}}
 * @example
 * {{{
 *   create table SpecialSecurities (Symbol: String, price: Double)
 *   containing values ('AAPL', 202), ('AMD', 22), ('INTL', 56), ('AMZN', 671)
 * }}}
 */
case class TableType(columns: Seq[TableColumn],
                     capacity: Int = 0,
                     growthFactor: Double = 0.20,
                     isPointer: Boolean = false,
                     partitions: List[String] = Nil)
  extends AbstractDataType(name = "Table") with Decoder[RowCollection] with RecordStructure with RowOrientedSupport {

  override def convert(value: Any): RowCollection = value.normalize match {
    case Some(v) => convert(v)
    case a: Array[_] => convert(a.toList)
    case rc: RowCollection => rc
    case jsonString: String => fromJSON(jsonString)
    case jsValue: JsValue => convert(jsValue.compactPrint)
    case seqMap: Seq[QMap[_, _]] => fromSeq(seqMap)
    case m: QMap[_, _] => m.map { case (k, v) => k.toString -> v }.toRow(columns).toRowCollection
    case x => dieUnsupportedConversion(x, name)
  }

  override def decode(buf: ByteBuffer): RowCollection = {
    // decode the buffer
    val deviceSize = buf.getInt
    val watermark = buf.getInt
    val bytes = new Array[Byte](deviceSize)
    buf.get(bytes)

    // return as a memory-based row collection
    val rc = new ByteArrayRowCollection(createTempNS(), columns, bytes)
    rc.watermark = watermark
    rc
  }

  override def encodeValue(value: Any): Array[Byte] = {
    convert(value) match {
      case rc: RowCollection =>
        rc.encode ~> { buf =>
          val deviceSize = buf.length
          val watermark = rc.getLength.toInt * recordSize
          allocate(maxSizeInBytes max buf.length).putInt(deviceSize).putInt(watermark).put(buf).flipMe().array()
        }
      case x => dieUnsupportedConversion(x, name)
    }
  }

  def fromJSON(jsonString: String): RowCollection = {
    import spray.json._
    val rows: Seq[Row] = jsonString.parseJson match {
      // a collection of records?
      case ja: JsArray =>
        val items = ja.elements map {
          case jo: JsObject => jo.fields.map { case (name, js) => name -> js.unwrapJSON }
          case x => dieUnsupportedConversion(x, name)
        }
        items.zipWithIndex map { case (mapping, _id) => makeRow(_id, mapping) }
      // a single record?
      case jo: JsObject =>
        val mapping = jo.fields.map { case (name, js) => name -> js.unwrapJSON }
        Seq(makeRow(id = 0, mapping))
      // unrecognized
      case x => dieUnsupportedConversion(x, name)
    }

    // create a new device containing the rows
    val device = ByteArrayRowCollection(createTempNS(), this)
    rows.foreach(device.insert)
    device
  }

  def fromSeq(seqMap: Seq[QMap[_, _]]): RowCollection = {
    val coll: Seq[QMap[String, Any]] = seqMap.map(_ map {
      case (k: String, v) => k -> v
      case (k, v) => String.valueOf(k) -> v
    })
    implicit val device: RowCollection = createTempTable(columns, fixedRowCount = coll.size)
    val rows = coll.map(_.toRow)
    rows.foreach(device.insert)
    device
  }

  override def getJDBCType: Int = java.sql.Types.STRUCT

  def isBlobTable: Boolean = isPointer

  def isClustered: Boolean = !isBlobTable & !isMultiTenant

  override def isExternal: Boolean = isBlobTable || isMultiTenant

  def isMultiTenant: Boolean = !isPointer & (capacity == 0)

  override def isTable: Boolean = true

  /**
   * headerSize = sizeOf(deviceSize) + sizeOf(watermark)
   * recordSize = sizeOf(rmd) + sizeOf(columns)
   * maxSizeInBytes = headerSize + capacity * recordSize
   */
  override val maxSizeInBytes: Int = headerSize + (capacity max 1) * recordSize

  /**
   * maxPhysicalSize = sizeOf(fmd) + maxSizeInBytes
   */
  override val maxPhysicalSize: Int = FieldMetadata.BYTES_LENGTH + maxSizeInBytes

  override def toColumnType: ColumnType = ColumnType.table(columns = columns.map(_.toColumn), capacity = capacity)

  override def toJavaType(hasNulls: Boolean): Class[_] = classOf[RowCollection]

  override def toSQL: String = {
    (List(name, columns.map(_.toColumn.toSQL).mkString("(", ", ", ")")) :::
      (if (capacity > 0) s"[$capacity]" else "") :: (if (isPointer) "*" else "") :: Nil).mkString
  }

  private def makeRow(id: ROWID, mapping: Map[String, Any]): Row = {
    Row(id, RowMetadata(), columns, fields = columns map { column =>
      Field(name = column.name, metadata = FieldMetadata(), value = mapping.get(column.name))
    })
  }

}

/**
 * Table Type Companion
 */
object TableType extends ColumnTypeParser with DataTypeParser {
  val headerSize: Int = 2 * INT_BYTES

  override def getCompatibleType(`class`: Class[_]): Option[DataType] = None

  override def getCompatibleValue(value: Any): Option[DataType] = value match {
    case t: RowCollection => Some(TableType(t.columns, capacity = t.getLength.toInt))
    case _ => None
  }

  override def parseColumnType(stream: TokenStream)(implicit compiler: SQLCompiler): Option[ColumnType] = {
    if (understands(stream)) {
      val typeName = stream.next().valueAsString
      Option(ColumnType(typeName).copy(nestedColumns =
        stream.captureIf("(", ")", delimiter = Some(","))(compiler.nextListOfParameters).flatten))
    } else None
  }

  override def parseDataType(columnType: ColumnType)(implicit scope: Scope): Option[DataType] = {
    columnType.name match {
      case s if synonyms.contains(s) =>
        Some(TableType(
          columns = columnType.nestedColumns.map(_.toTableColumn),
          capacity = if (columnType.arrayArgs.nonEmpty) columnType.arrayArgs.map(_.toInt).product else 0,
          isPointer = columnType.isPointer
        ))
      case _ => None
    }
  }

  override def help: List[HelpDoc] = Nil

  override def synonyms: Set[String] = Set("Table")

  override def understands(ts: TokenStream)(implicit compiler: SQLCompiler): Boolean = synonyms.exists(ts is _)

  final implicit class TableTypeRefExtensions(val tableModel: TableModel) extends AnyVal {

    @inline
    def toTableType(implicit scope: Scope): TableType = {
      TableType(
        columns = tableModel.columns.map(_.toTableColumn),
        capacity = tableModel.initialCapacity || 0,
        growthFactor = 0.2,
        isPointer = false,
        partitions = for {
          partitions <- tableModel.partitions.toList
          array = partitions.pullArray._3.toList
          list <- array.map {
            case s: String => s
            case x => partitions.dieIllegalType(x)
          }
        } yield list)
    }
  }

}