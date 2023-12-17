package com.lollypop.database
package jdbc

import com.lollypop.database.jdbc.JDBCResultSet.UNINITED
import com.lollypop.database.jdbc.types._
import com.lollypop.language._
import com.lollypop.runtime._
import com.lollypop.runtime.datatypes._
import com.lollypop.runtime.devices.{RowCollection, TableColumn}
import lollypop.io.{Encodable, IOCost}

import java.io.{InputStream, Reader}
import java.math.RoundingMode
import java.net.{URI, URL}
import java.nio.ByteBuffer
import java.sql.{ResultSet, Statement}
import java.util
import java.util.{Calendar, UUID}
import scala.beans.{BeanProperty, BooleanBeanProperty}

/**
 * Lollypop JDBC Result Set
 * @param connection the [[JDBCConnection connection]]
 * @param ns         the [[DatabaseObjectNS realized database object namespace]]
 * @param columns    the [[TableColumn table columns]]
 * @param rows       the collection of row data
 * @param cost       the [[IOCost I/O cost]]
 * @param __ids      the collection of row identifiers
 */
class JDBCResultSet(connection: JDBCConnection,
                    ns: DatabaseObjectNS,
                    columns: Seq[TableColumn],
                    var rows: Seq[Seq[Any]],
                    var cost: Option[IOCost] = None,
                    var __ids: Seq[ROWID] = Nil)
  extends ResultSet with JDBCWrapper {
  private val matrix: Array[Any] = rows.flatten.toArray
  private var lastInsertedRowIndex: Int = UNINITED
  private var rowIndex: Int = UNINITED

  @BooleanBeanProperty var closed: Boolean = false
  @BeanProperty val concurrency: Int = ResultSet.CONCUR_UPDATABLE
  @BeanProperty val cursorName: String = UUID.randomUUID().toString
  @BeanProperty var fetchDirection: Int = ResultSet.FETCH_FORWARD
  @BeanProperty var fetchSize: Int = 20
  @BeanProperty val holdability: Int = ResultSet.HOLD_CURSORS_OVER_COMMIT
  @BeanProperty val metaData = new JDBCResultSetMetaData(ns, columns)
  @BeanProperty val `type`: Int = ResultSet.TYPE_SCROLL_SENSITIVE
  @BeanProperty val warnings = new java.sql.SQLWarning()

  override def close(): Unit = closed = true

  override def findColumn(columnLabel: String): Int = {
    val index = columns.indexWhere(_.name == columnLabel)
    assert(index != -1, die(s"Column '$columnLabel' not found"))
    index + 1
  }

  override def getBoolean(columnIndex: Int): Boolean = convertAny(columnIndex, typeName = "Boolean", {
    case b: Boolean => b
    case b: java.lang.Boolean => b
  })

  override def getByte(columnIndex: Int): Byte = convertNumeric(columnIndex, typeName = "Byte", _.byteValue())

  override def getShort(columnIndex: Int): Short = convertNumeric(columnIndex, typeName = "Short", _.shortValue())

  override def getInt(columnIndex: Int): Int = convertNumeric(columnIndex, typeName = "Int", _.intValue())

  override def getLong(columnIndex: Int): Long = convertNumeric(columnIndex, typeName = "Long", _.longValue())

  override def getFloat(columnIndex: Int): Float = convertNumeric(columnIndex, typeName = "Float", _.floatValue())

  override def getDouble(columnIndex: Int): Double = convertNumeric(columnIndex, typeName = "Double", _.doubleValue())

  @deprecated
  override def getBigDecimal(columnIndex: Int, scale: Int): java.math.BigDecimal = {
    Option(getBigDecimal(columnIndex)).map(_.setScale(scale, RoundingMode.UP)).orNull
  }

  override def getBytes(columnIndex: Int): Array[Byte] = {
    getColumnValue[AnyRef](columnIndex) match {
      case a: Array[Byte] => a
      case c: Array[Char] => String.valueOf(c).getBytes()
      case e: Encodable => e.encode
      case i: InputStream => i.readAllBytes()
      case r: Reader => r.mkString().getBytes()
      case s: String => s.getBytes()
      // JDBC types
      case b: java.sql.Blob => b.getBinaryStream.readAllBytes()
      case c: java.sql.Clob => c.getAsciiStream.readAllBytes()
      case s: java.sql.SQLXML => s.getBinaryStream.readAllBytes()
      case x => dieUnsupportedConversion(x, "byte[]")
    }
  }

  override def getAsciiStream(columnIndex: Int): InputStream = getClob(columnIndex).getAsciiStream

  @deprecated
  override def getUnicodeStream(columnIndex: Int): InputStream = getClob(columnIndex).getAsciiStream

  override def getClob(columnIndex: Int): java.sql.Clob = getNClob(columnIndex)

  override def getBinaryStream(columnIndex: Int): InputStream = getBlob(columnIndex).getBinaryStream

  override def getBoolean(columnLabel: String): Boolean = getBoolean(findColumn(columnLabel))

  override def getByte(columnLabel: String): Byte = getByte(findColumn(columnLabel))

  override def getShort(columnLabel: String): Short = getShort(findColumn(columnLabel))

  override def getInt(columnLabel: String): Int = getInt(findColumn(columnLabel))

  override def getLong(columnLabel: String): Long = getLong(findColumn(columnLabel))

  override def getFloat(columnLabel: String): Float = getFloat(findColumn(columnLabel))

  override def getDouble(columnLabel: String): Double = getDouble(findColumn(columnLabel))

  @deprecated
  override def getBigDecimal(columnLabel: String, scale: Int): java.math.BigDecimal = getBigDecimal(findColumn(columnLabel), scale)

  override def getBytes(columnLabel: String): Array[Byte] = getBytes(findColumn(columnLabel))

  override def getAsciiStream(columnLabel: String): InputStream = getAsciiStream(findColumn(columnLabel))

  override def getClob(columnLabel: String): java.sql.Clob = getClob(findColumn(columnLabel))

  @deprecated
  override def getUnicodeStream(columnLabel: String): InputStream = getUnicodeStream(findColumn(columnLabel))

  override def getBinaryStream(columnLabel: String): InputStream = getBinaryStream(findColumn(columnLabel))

  override def getBlob(columnLabel: String): java.sql.Blob = getBlob(findColumn(columnLabel))

  override def getBlob(columnIndex: Int): java.sql.Blob = {
    getColumnValue[Any](columnIndex) match {
      case b: Array[Byte] => BLOB.fromBytes(b)
      case e: Encodable => BLOB.fromBytes(e.encode)
      case i: InputStream => BLOB.fromInputStream(i)
      case r: Reader => BLOB.fromReader(r)
      case s: String => BLOB.fromString(s)
      // JDBC types
      case b: java.sql.Blob => b
      case c: java.sql.Clob => BLOB.fromInputStream(c.getAsciiStream)
      case s: java.sql.SQLXML => BLOB.fromInputStream(s.getBinaryStream)
      case x => dieUnsupportedConversion(x, "Blob")
    }
  }

  override def clearWarnings(): Unit = ()

  override def getObject(columnIndex: Int): AnyRef = {
    getColumnValue[AnyRef](columnIndex) match {
      case mx: Matrix => JDBCStruct(dataType = mx.returnType, attributes = mx.toArray.asInstanceOf[Array[AnyRef]])
      case rc: RowCollection => JDBCStruct(dataType = rc.returnType, attributes = rc.toMapGraph.toArray)
      case x => x
    }
  }

  override def getObject(columnLabel: String): AnyRef = getObject(findColumn(columnLabel))

  override def getBigDecimal(columnIndex: Int): java.math.BigDecimal = {
    convertNumeric(columnIndex, typeName = "BigDecimal", {
      case b: scala.math.BigDecimal => b.bigDecimal
      case b: java.math.BigDecimal => b
      case n => new java.math.BigDecimal(n.doubleValue())
    })
  }

  override def getBigDecimal(columnLabel: String): java.math.BigDecimal = getBigDecimal(findColumn(columnLabel))

  override def isBeforeFirst: Boolean = rowIndex < 0

  override def isAfterLast: Boolean = rowIndex >= rows.length

  override def isFirst: Boolean = rowIndex == 0

  override def isLast: Boolean = rowIndex == rows.length - 1

  override def beforeFirst(): Unit = rowIndex = UNINITED

  override def afterLast(): Unit = rowIndex = rows.length

  override def first(): Boolean = {
    rowIndex = if (rows.isEmpty) UNINITED else 0
    isValidRow
  }

  override def last(): Boolean = {
    rowIndex = (rows.length - 1) max UNINITED
    isValidRow
  }

  override def getRow: Int = rowIndex + 1

  override def absolute(row: Int): Boolean = {
    val isOkay = row < rows.length
    if (isOkay) rowIndex = row - 1
    isOkay
  }

  override def next(): Boolean = {
    rowIndex += 1
    rowIndex < rows.length
  }

  override def previous(): Boolean = {
    if (rowIndex < 0) false else {
      rowIndex -= 1
      true
    }
  }

  override def relative(rowDelta: Int): Boolean = {
    val newIndex = rowIndex + rowDelta
    val isOkay = newIndex >= 0 && newIndex < this.rows.length
    if (isOkay) rowIndex = newIndex
    isOkay
  }

  override def rowUpdated(): Boolean = cost.exists(_.updated > 0)

  override def rowInserted(): Boolean = cost.exists(_.inserted > 0)

  override def rowDeleted(): Boolean = cost.exists(_.deleted > 0)

  override def updateNull(columnIndex: Int): Unit = matrix(getOffset(columnIndex)) = null

  override def updateBoolean(columnIndex: Int, x: Boolean): Unit = update(columnIndex, x)

  override def updateByte(columnIndex: Int, x: Byte): Unit = update(columnIndex, x)

  override def updateShort(columnIndex: Int, x: Short): Unit = update(columnIndex, x)

  override def updateInt(columnIndex: Int, x: Int): Unit = update(columnIndex, x)

  override def updateLong(columnIndex: Int, x: Long): Unit = update(columnIndex, x)

  override def updateFloat(columnIndex: Int, x: Float): Unit = update(columnIndex, x)

  override def updateDouble(columnIndex: Int, x: Double): Unit = update(columnIndex, x)

  override def updateBigDecimal(columnIndex: Int, x: java.math.BigDecimal): Unit = update(columnIndex, x)

  override def updateBytes(columnIndex: Int, x: Array[Byte]): Unit = update(columnIndex, x)

  override def updateDate(columnIndex: Int, x: java.sql.Date): Unit = update(columnIndex, x)

  override def updateTime(columnIndex: Int, x: java.sql.Time): Unit = update(columnIndex, x)

  override def updateTimestamp(columnIndex: Int, x: java.sql.Timestamp): Unit = update(columnIndex, x)

  override def updateAsciiStream(columnIndex: Int, x: InputStream, length: Int): Unit = update(columnIndex, x.limitTo(length))

  override def updateBinaryStream(columnIndex: Int, x: InputStream, length: Int): Unit = update(columnIndex, x.limitTo(length))

  override def updateCharacterStream(columnIndex: Int, x: Reader, length: Int): Unit = update(columnIndex, x.limitTo(length))

  override def updateObject(columnIndex: Int, x: Any, scaleOrLength: Int): Unit = {
    update(columnIndex, x match {
      case bd: java.math.BigDecimal => bd.setScale(scaleOrLength, RoundingMode.UP)
      case bd: scala.math.BigDecimal => bd.bigDecimal.setScale(scaleOrLength, RoundingMode.UP)
      case xx => xx
    })
  }

  override def updateObject(columnIndex: Int, x: Any): Unit = update(columnIndex, x)

  override def updateNull(columnLabel: String): Unit = updateNull(findColumn(columnLabel))

  override def updateBoolean(columnLabel: String, x: Boolean): Unit = updateBoolean(findColumn(columnLabel), x)

  override def updateByte(columnLabel: String, x: Byte): Unit = updateByte(findColumn(columnLabel), x)

  override def updateShort(columnLabel: String, x: Short): Unit = updateShort(findColumn(columnLabel), x)

  override def updateInt(columnLabel: String, x: Int): Unit = updateInt(findColumn(columnLabel), x)

  override def updateLong(columnLabel: String, x: Long): Unit = updateLong(findColumn(columnLabel), x)

  override def updateFloat(columnLabel: String, x: Float): Unit = updateFloat(findColumn(columnLabel), x)

  override def updateDouble(columnLabel: String, x: Double): Unit = updateDouble(findColumn(columnLabel), x)

  override def updateBigDecimal(columnLabel: String, x: java.math.BigDecimal): Unit = updateBigDecimal(findColumn(columnLabel), x)

  override def updateBytes(columnLabel: String, x: Array[Byte]): Unit = updateBytes(findColumn(columnLabel), x)

  override def updateDate(columnLabel: String, x: java.sql.Date): Unit = updateDate(findColumn(columnLabel), x)

  override def updateTime(columnLabel: String, x: java.sql.Time): Unit = updateTime(findColumn(columnLabel), x)

  override def updateTimestamp(columnLabel: String, x: java.sql.Timestamp): Unit = updateTimestamp(findColumn(columnLabel), x)

  override def updateAsciiStream(columnLabel: String, x: InputStream, length: Int): Unit = {
    updateAsciiStream(findColumn(columnLabel), x.limitTo(length))
  }

  override def updateBinaryStream(columnLabel: String, x: InputStream, length: Int): Unit = {
    updateBinaryStream(findColumn(columnLabel), x, length)
  }

  override def updateCharacterStream(columnLabel: String, reader: Reader, length: Int): Unit = {
    updateCharacterStream(findColumn(columnLabel), reader, length)
  }

  override def updateObject(columnLabel: String, x: Any, scaleOrLength: Int): Unit = {
    updateObject(findColumn(columnLabel), x, scaleOrLength)
  }

  override def updateObject(columnLabel: String, x: Any): Unit = updateObject(findColumn(columnLabel), x)

  override def insertRow(): Unit = {
    cost = Option {
      val newRow = constructRow
      val w = connection.client.insertRow(ns.databaseName, ns.schemaName, ns.name, newRow)
      if (w.inserted > 0) {
        lastInsertedRowIndex = rows.length
        rows = rows.toList ::: (newRow.values.toList :: Nil)
        __ids = __ids.toList ::: w.rowIDs.toList
      }
      w
    }
  }

  override def updateRow(): Unit = {
    cost = __id().map(connection.client.replaceRow(ns.databaseName, ns.schemaName, ns.name, _, constructRow))
  }

  override def deleteRow(): Unit = {
    cost = __id().map(connection.client.deleteRow(ns.databaseName, ns.schemaName, ns.name, _))
  }

  override def refreshRow(): Unit = {
    val refreshedRow = for {
      rowID <- __id().toArray
      row <- connection.client.getRow(ns.databaseName, ns.schemaName, ns.name, rowID).toArray
      name <- columns.map(_.name)
    } yield row.get(name)

    // overwrite the slice
    val p0 = rowIndex * columns.length
    val p1 = (rowIndex + 1) * columns.length
    System.arraycopy(refreshedRow, 0, matrix, p0, p1 - p0)
  }

  override def cancelRowUpdates(): Unit = refreshRow()

  override def moveToInsertRow(): Unit = rowIndex = lastInsertedRowIndex

  override def moveToCurrentRow(): Unit = rowIndex = rows.length - 1

  override def getStatement: Statement = new JDBCStatement(connection)

  override def getObject(columnIndex: Int, map: util.Map[String, Class[_]]): AnyRef = {
    getObject(columnIndex)
  }

  override def getRef(columnIndex: Int): java.sql.Ref = JDBCRef(getColumnValue[Any](columnIndex))

  override def getArray(columnIndex: Int): java.sql.Array = prepareArray(getColumnValueOpt[AnyRef](columnIndex))

  override def getObject(columnLabel: String, map: util.Map[String, Class[_]]): AnyRef = {
    getObject(findColumn(columnLabel), map)
  }

  override def getRef(columnLabel: String): java.sql.Ref = getRef(findColumn(columnLabel))

  override def getArray(columnLabel: String): java.sql.Array = getArray(findColumn(columnLabel))

  override def getDate(columnIndex: Int, cal: Calendar): java.sql.Date = {
    (Option(getDate(columnIndex)) map { date =>
      cal.setTime(date)
      new java.sql.Date(cal.getTimeInMillis)
    }).orNull
  }

  override def getDate(columnIndex: Int): java.sql.Date = {
    convertDateTime(columnIndex, typeName = "Date", new java.sql.Date(_))
  }

  override def getDate(columnLabel: String, cal: Calendar): java.sql.Date = getDate(findColumn(columnLabel), cal)

  override def getDate(columnLabel: String): java.sql.Date = getDate(findColumn(columnLabel))

  override def getTime(columnIndex: Int, cal: Calendar): java.sql.Time = getTime(columnIndex)

  override def getTime(columnIndex: Int): java.sql.Time = {
    convertDateTime(columnIndex, typeName = "Time", new java.sql.Time(_))
  }

  override def getTime(columnLabel: String, cal: Calendar): java.sql.Time = getTime(columnLabel)

  override def getTime(columnLabel: String): java.sql.Time = getTime(findColumn(columnLabel))

  override def getTimestamp(columnIndex: Int, cal: Calendar): java.sql.Timestamp = getTimestamp(columnIndex)

  override def getTimestamp(columnIndex: Int): java.sql.Timestamp = {
    convertDateTime(columnIndex, typeName = "Timestamp", new java.sql.Timestamp(_))
  }

  override def getTimestamp(columnLabel: String, cal: Calendar): java.sql.Timestamp = getTimestamp(columnLabel)

  override def getTimestamp(columnLabel: String): java.sql.Timestamp = getTimestamp(findColumn(columnLabel))

  override def getURL(columnIndex: Int): URL = getColumnValueOpt[String](columnIndex).map(new URI(_).toURL).orNull

  override def getURL(columnLabel: String): URL = getURL(findColumn(columnLabel))

  override def updateRef(columnIndex: Int, x: java.sql.Ref): Unit = update(columnIndex, x)

  override def updateRef(columnLabel: String, x: java.sql.Ref): Unit = updateRef(findColumn(columnLabel), x)

  override def updateBlob(columnIndex: Int, x: java.sql.Blob): Unit = update(columnIndex, x)

  override def updateBlob(columnLabel: String, x: java.sql.Blob): Unit = updateBlob(findColumn(columnLabel), x)

  override def updateArray(columnIndex: Int, x: java.sql.Array): Unit = update(columnIndex, x)

  override def updateArray(columnLabel: String, x: java.sql.Array): Unit = updateArray(findColumn(columnLabel), x)

  override def getRowId(columnIndex: Int): java.sql.RowId = {
    validateColumnIndex(columnIndex)
    if (__ids.nonEmpty) JDBCRowId(__ids(rowIndex)) else null
  }

  override def getRowId(columnLabel: String): java.sql.RowId = getRowId(findColumn(columnLabel))

  override def updateRowId(columnIndex: Int, x: java.sql.RowId): Unit = update(columnIndex, x)

  override def updateRowId(columnLabel: String, x: java.sql.RowId): Unit = updateRowId(findColumn(columnLabel), x)

  override def updateNString(columnIndex: Int, nString: String): Unit = updateString(columnIndex, nString)

  override def updateString(columnIndex: Int, x: String): Unit = update(columnIndex, x)

  override def updateNString(columnLabel: String, nString: String): Unit = updateString(findColumn(columnLabel), nString)

  override def updateString(columnLabel: String, x: String): Unit = updateString(findColumn(columnLabel), x)

  override def updateNClob(columnIndex: Int, nClob: java.sql.NClob): Unit = updateClob(columnIndex, nClob)

  override def updateClob(columnIndex: Int, x: java.sql.Clob): Unit = update(columnIndex, x)

  override def updateNClob(columnLabel: String, nClob: java.sql.NClob): Unit = updateClob(findColumn(columnLabel), nClob)

  override def updateClob(columnLabel: String, x: java.sql.Clob): Unit = updateClob(findColumn(columnLabel), x)

  override def getNClob(columnIndex: Int): java.sql.NClob = {
    getColumnValue[Any](columnIndex) match {
      case b: Array[Byte] => CLOB.fromString(new String(b))
      case c: Array[Char] => CLOB.fromChars(c)
      case i: InputStream => CLOB.fromInputStream(i)
      case r: Reader => CLOB.fromReader(r)
      case s: String => CLOB.fromString(s)
      // JDBC types
      case b: java.sql.Blob => CLOB.fromInputStream(b.getBinaryStream)
      case c: java.sql.NClob => c
      case c: java.sql.Clob => CLOB.fromInputStream(c.getAsciiStream)
      case s: java.sql.SQLXML => CLOB.fromInputStream(s.getBinaryStream)
      case x => dieUnsupportedConversion(x, "NClob")
    }
  }

  override def getNClob(columnLabel: String): java.sql.NClob = getNClob(findColumn(columnLabel))

  override def getSQLXML(columnIndex: Int): java.sql.SQLXML = {
    getColumnValue[Any](columnIndex) match {
      case b: Array[Byte] => SQLXML.fromBytes(b)
      case c: Array[Char] => SQLXML.fromChars(c)
      case i: InputStream => SQLXML.fromInputStream(i)
      case r: Reader => SQLXML.fromReader(r)
      case s: String => SQLXML.fromString(s)
      // JDBC types
      case b: java.sql.Blob => SQLXML.fromInputStream(b.getBinaryStream)
      case c: java.sql.Clob => SQLXML.fromInputStream(c.getAsciiStream)
      case s: java.sql.SQLXML => s
      case x => dieUnsupportedConversion(x, "SQLXML")
    }
  }

  override def getSQLXML(columnLabel: String): java.sql.SQLXML = getSQLXML(findColumn(columnLabel))

  override def updateSQLXML(columnIndex: Int, xmlObject: java.sql.SQLXML): Unit = update(columnIndex, xmlObject)

  override def updateSQLXML(columnLabel: String, xmlObject: java.sql.SQLXML): Unit = {
    updateSQLXML(findColumn(columnLabel), xmlObject)
  }

  override def getNString(columnIndex: Int): String = getString(columnIndex)

  override def getString(columnIndex: Int): String = getColumnValueOpt[String](columnIndex).orNull

  override def getNString(columnLabel: String): String = getNString(findColumn(columnLabel))

  override def getString(columnLabel: String): String = getString(findColumn(columnLabel))

  override def getNCharacterStream(columnIndex: Int): Reader = getCharacterStream(columnIndex)

  override def getCharacterStream(columnIndex: Int): Reader = getClob(columnIndex).getCharacterStream

  override def getNCharacterStream(columnLabel: String): Reader = getNCharacterStream(findColumn(columnLabel))

  override def getCharacterStream(columnLabel: String): Reader = getCharacterStream(findColumn(columnLabel))

  override def updateNCharacterStream(columnIndex: Int, x: Reader, length: Long): Unit = updateCharacterStream(columnIndex, x, length)

  override def updateCharacterStream(columnIndex: Int, x: Reader, length: Long): Unit = update(columnIndex, x.limitTo(length))

  override def updateNCharacterStream(columnLabel: String, reader: Reader, length: Long): Unit = {
    updateNCharacterStream(findColumn(columnLabel), reader, length)
  }

  override def updateCharacterStream(columnLabel: String, reader: Reader, length: Long): Unit = {
    updateCharacterStream(findColumn(columnLabel), reader)
  }

  override def updateAsciiStream(columnIndex: Int, x: InputStream, length: Long): Unit = update(columnIndex, x)

  override def updateBinaryStream(columnIndex: Int, x: InputStream, length: Long): Unit = update(columnIndex, x.toBase64)

  override def updateAsciiStream(columnLabel: String, x: InputStream, length: Long): Unit = {
    updateAsciiStream(findColumn(columnLabel), x)
  }

  override def updateBinaryStream(columnLabel: String, x: InputStream, length: Long): Unit = {
    updateBinaryStream(findColumn(columnLabel), x, length)
  }

  override def updateBlob(columnIndex: Int, inputStream: InputStream, length: Long): Unit = update(columnIndex, inputStream)

  override def updateBlob(columnLabel: String, inputStream: InputStream, length: Long): Unit = {
    updateBlob(findColumn(columnLabel), inputStream, length)
  }

  override def updateNClob(columnIndex: Int, reader: Reader, length: Long): Unit = updateClob(columnIndex, reader, length)

  override def updateClob(columnIndex: Int, reader: Reader, length: Long): Unit = update(columnIndex, reader)

  override def updateNClob(columnLabel: String, reader: Reader, length: Long): Unit = {
    updateNClob(findColumn(columnLabel), reader, length)
  }

  override def updateClob(columnLabel: String, reader: Reader, length: Long): Unit = {
    updateClob(findColumn(columnLabel), reader)
  }

  override def updateNCharacterStream(columnIndex: Int, x: Reader): Unit = updateCharacterStream(columnIndex, x)

  override def updateCharacterStream(columnIndex: Int, x: Reader): Unit = update(columnIndex, x)

  override def updateNCharacterStream(columnLabel: String, reader: Reader): Unit = {
    updateNCharacterStream(findColumn(columnLabel), reader)
  }

  override def updateCharacterStream(columnLabel: String, reader: Reader): Unit = {
    updateCharacterStream(findColumn(columnLabel), reader)
  }

  override def updateAsciiStream(columnIndex: Int, x: InputStream): Unit = update(columnIndex, x)

  override def updateBinaryStream(columnIndex: Int, x: InputStream): Unit = update(columnIndex, x)

  override def updateAsciiStream(columnLabel: String, x: InputStream): Unit = updateAsciiStream(findColumn(columnLabel), x)

  override def updateBinaryStream(columnLabel: String, x: InputStream): Unit = updateBinaryStream(findColumn(columnLabel), x)

  override def updateBlob(columnIndex: Int, inputStream: InputStream): Unit = update(columnIndex, inputStream)

  override def updateBlob(columnLabel: String, inputStream: InputStream): Unit = updateBlob(findColumn(columnLabel), inputStream)

  override def updateNClob(columnIndex: Int, reader: Reader): Unit = updateClob(columnIndex, reader)

  override def updateClob(columnIndex: Int, reader: Reader): Unit = update(columnIndex, reader)

  override def updateNClob(columnLabel: String, reader: Reader): Unit = updateNClob(findColumn(columnLabel), reader)

  override def updateClob(columnLabel: String, reader: Reader): Unit = updateClob(findColumn(columnLabel), reader)

  override def getObject[T](columnIndex: Int, `type`: Class[T]): T = {
    Option(getObject(columnIndex)).flatMap(safeCast[T]).getOrElse(null.asInstanceOf[T])
  }

  override def getObject[T](columnLabel: String, `type`: Class[T]): T = {
    getObject[T](findColumn(columnLabel), `type`)
  }

  override def wasNull(): Boolean = rows.isEmpty

  private def constructRow: Map[String, Any] = {
    val p0 = rowIndex * columns.length
    val p1 = (rowIndex + 1) * columns.length
    Map((for {
      n <- p0 until p1
      name = columns(n - p0).name
      value = matrix(n)
    } yield name -> value): _*)
  }

  private def convertAny[A](columnIndex: Int, typeName: String, f: PartialFunction[Any, A]): A = {
    val value = getColumnValue[Any](columnIndex)
    if (f.isDefinedAt(value)) f(value) else dieUnsupportedConversion(value, typeName)
  }

  private def convertDateTime[A](columnIndex: Int, typeName: String, f: Long => A): A = {
    val ts = convertAny(columnIndex, typeName, {
      case n: Number => n.longValue()
      case d: java.util.Date => d.getTime
    })
    f(ts)
  }

  private def convertNumeric[A](columnIndex: Int, typeName: String, f: Number => A): A = {
    getColumnValue[Any](columnIndex) match {
      case n: Number => f(n)
      case x => dieUnsupportedConversion(x, typeName)
    }
  }

  private def getColumnValue[T](columnIndex: Int): T = {
    getColumnValueOpt[T](columnIndex).getOrElse(null.asInstanceOf[T])
  }

  private def getColumnValueOpt[T](columnIndex: Int): Option[T] = {
    val offset = getOffset(columnIndex)

    // lookup the value by column name
    val column = columns(columnIndex - 1)
    val rawValue = if (offset < matrix.length) matrix(offset) else null
    safeCast[T](column.`type`.convert(rawValue))
  }

  private def getOffset(columnIndex: Int): Int = {
    assert(rowIndex >= 0 && rowIndex < rows.length, die("Invalid cursor position. Did you call ResultSet.next()?"))
    validateColumnIndex(columnIndex)
    val offset = rowIndex * columns.length + (columnIndex - 1)
    assert(offset >= 0 && offset < rows.length * columns.length, die(s"Invalid offset ($offset)"))
    offset
  }

  private def __id(columnIndex: Int = 1): Option[ROWID] = {
    Option(getRowId(columnIndex)).map(r => ByteBuffer.wrap(r.getBytes).getRowID)
  }

  private def isValidRow: Boolean = rowIndex < rows.length

  private def prepareArray(value_? : Option[AnyRef]): java.sql.Array = {
    val value = value_? map {
      case array: Array[_] =>
        JDBCArray(connection, typeName = "Any", array.asInstanceOf[Array[AnyRef]])
      case device: RowCollection =>
        val columns = device.columns.map(_.name)
        val array = (device.toMapGraph map { mapping =>
          val attributes = columns.map(name => mapping.get(name).orNull).toArray
          JDBCStruct(dataType = Inferences.fromValue(attributes), attributes.asInstanceOf[Array[AnyRef]])
        }).toArray
        types.JDBCArray(connection, typeName = "Any", array)
      case x => dieUnsupportedConversion(x, "byte[]")
    }
    value.orNull
  }

  private def update(columnIndex: Int, value: Any): Unit = {
    matrix(getOffset(columnIndex)) = value match {
      case o: Option[_] => o.orNull
      case v => v
    }
  }

  private def validateColumnIndex(columnIndex: Int): Unit = {
    assert(columnIndex > 0 && columnIndex <= columns.length, die(s"Column index is out of range ($columnIndex)"))
  }

}

/**
 * Lollypop Result Set Companion
 */
object JDBCResultSet {
  private val UNINITED = -1

  /**
   * Creates a new [[JDBCResultSet Lollypop result set]]
   * @param queryResult the [[QueryResponse]]
   * @return a new [[JDBCResultSet Lollypop result set]]
   */
  def apply(connection: JDBCConnection, queryResult: QueryResponse) = new JDBCResultSet(
    connection = connection,
    ns = queryResult.ns,
    columns = queryResult.columns,
    cost = queryResult.cost,
    rows = queryResult.rows,
    __ids = queryResult.__ids
  )

  def apply(connection: JDBCConnection,
            databaseName: String,
            schemaName: String,
            tableName: String,
            columns: Seq[TableColumn],
            data: Seq[Seq[Any]],
            __ids: Seq[ROWID] = Nil,
            cost: Option[IOCost] = None): JDBCResultSet = {
    new JDBCResultSet(
      connection = connection,
      ns = DatabaseObjectNS(databaseName, schemaName, tableName),
      columns = columns,
      cost = cost,
      rows = data,
      __ids = __ids
    )
  }

}