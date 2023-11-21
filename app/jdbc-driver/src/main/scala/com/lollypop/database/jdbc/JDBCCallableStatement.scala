package com.lollypop.database.jdbc

import com.lollypop.database.jdbc.types.JDBCValueConversion._
import com.lollypop.database.jdbc.types._
import com.lollypop.runtime.datatypes._
import com.lollypop.runtime.conversions.TransferTools.{RichInputStream, RichReader}

import java.io.{InputStream, Reader}
import java.net.{URI, URL}
import java.util
import java.util.Calendar
import scala.beans.BeanProperty

/**
 * Lollypop Callable Statement
 * @param connection the [[JDBCConnection connection]]
 * @param sql        the SQL query
 */
class JDBCCallableStatement(@BeanProperty connection: JDBCConnection, sql: String)
  extends JDBCPreparedStatement(connection, sql) with java.sql.CallableStatement {
  private val outParameters: JDBCOutParameters = new JDBCOutParameters()

  override def wasNull(): Boolean = false

  override def execute(): Boolean = {
    val outcome = super.execute()
    outParameters.updateResults(resultSet)
    outcome
  }

  override def executeQuery(): java.sql.ResultSet = {
    val outcome = super.executeQuery()
    outParameters.updateResults(resultSet)
    outcome
  }

  override def executeUpdate(): Int = {
    val outcome = super.executeUpdate()
    outParameters.updateResults(resultSet)
    outcome
  }

  override def getString(parameterIndex: Int): String = StringType.convert(getParameter(parameterIndex))

  override def getBoolean(parameterIndex: Int): Boolean = BooleanType.convert(getParameter(parameterIndex))

  override def getByte(parameterIndex: Int): Byte = Int8Type.convert(getParameter(parameterIndex))

  override def getShort(parameterIndex: Int): Short = Int16Type.convert(getParameter(parameterIndex))

  override def getInt(parameterIndex: Int): Int = Int32Type.convert(getParameter(parameterIndex))

  override def getLong(parameterIndex: Int): Long = Int64Type.convert(getParameter(parameterIndex))

  override def getFloat(parameterIndex: Int): Float = Float32Type.convert(getParameter(parameterIndex))

  override def getDouble(parameterIndex: Int): Double = Float64Type.convert(getParameter(parameterIndex))

  override def getBigDecimal(parameterIndex: Int, scale: Int): java.math.BigDecimal = {
    new java.math.BigDecimal(NumericType.convert(getParameter(parameterIndex)).doubleValue()).setScale(scale)
  }

  override def getBytes(parameterIndex: Int): Array[Byte] = VarBinaryType.convert(getParameter(parameterIndex))

  override def getDate(parameterIndex: Int): java.sql.Date = {
    new java.sql.Date(DateTimeType.convert(getParameter(parameterIndex)).getTime)
  }

  override def getTime(parameterIndex: Int): java.sql.Time = {
    new java.sql.Time(DateTimeType.convert(getParameter(parameterIndex)).getTime)
  }

  override def getTimestamp(parameterIndex: Int): java.sql.Timestamp = {
    new java.sql.Timestamp(DateTimeType.convert(getParameter(parameterIndex)).getTime)
  }

  override def getObject(parameterIndex: Int): Any = getParameter(parameterIndex)

  override def getBigDecimal(parameterIndex: Int): java.math.BigDecimal = {
    new java.math.BigDecimal(NumericType.convert(getParameter(parameterIndex)).doubleValue())
  }

  override def getObject(parameterIndex: Int, map: util.Map[String, Class[_]]): Any = getParameter(parameterIndex)

  override def getRef(parameterIndex: Int): java.sql.Ref = JDBCRef(getParameter(parameterIndex))

  override def getBlob(parameterIndex: Int): java.sql.Blob = BlobType.convert(getParameter(parameterIndex))

  override def getClob(parameterIndex: Int): java.sql.Clob = ClobType.convert(getParameter(parameterIndex))

  override def getArray(parameterIndex: Int): java.sql.Array = {
    JDBCArray(connection, getParameter(parameterIndex))
  }

  override def getDate(parameterIndex: Int, cal: Calendar): java.sql.Date = {
    new java.sql.Date(DateTimeType.convert(getParameter(parameterIndex)).getTime)
  }

  override def getTime(parameterIndex: Int, cal: Calendar): java.sql.Time = {
    new java.sql.Time(DateTimeType.convert(getParameter(parameterIndex)).getTime)
  }

  override def getTimestamp(parameterIndex: Int, cal: Calendar): java.sql.Timestamp = {
    new java.sql.Timestamp(DateTimeType.convert(getParameter(parameterIndex)).getTime)
  }

  override def registerOutParameter(parameterIndex: Int, sqlType: Int): Unit = {
    outParameters.register(parameterIndex, sqlType)
  }

  override def registerOutParameter(parameterIndex: Int, sqlType: Int, scale: Int): Unit = {
    outParameters.register(parameterIndex, sqlType)
  }

  override def registerOutParameter(parameterIndex: Int, sqlType: Int, typeName: String): Unit = {
    outParameters.register(parameterIndex, sqlType)
  }

  override def registerOutParameter(parameterName: String, sqlType: Int): Unit = {
    outParameters.register(parameterName, sqlType)
  }

  override def registerOutParameter(parameterName: String, sqlType: Int, scale: Int): Unit = {
    outParameters.register(parameterName, sqlType)
  }

  override def registerOutParameter(parameterName: String, sqlType: Int, typeName: String): Unit = {
    outParameters.register(parameterName, sqlType)
  }

  override def getURL(parameterIndex: Int): URL = new URI(StringType.convert(getParameter(parameterIndex))).toURL

  override def setURL(parameterName: String, url: URL): Unit = setParameter(parameterName, url.toExternalForm)

  override def setNull(parameterName: String, sqlType: Int): Unit = setParameter(parameterName, _null(sqlType))

  override def setBoolean(parameterName: String, x: Boolean): Unit = setParameter(parameterName, x)

  override def setByte(parameterName: String, x: Byte): Unit = setParameter(parameterName, x)

  override def setShort(parameterName: String, x: Short): Unit = setParameter(parameterName, x)

  override def setInt(parameterName: String, x: Int): Unit = setParameter(parameterName, x)

  override def setLong(parameterName: String, x: Long): Unit = setParameter(parameterName, x)

  override def setFloat(parameterName: String, x: Float): Unit = setParameter(parameterName, x)

  override def setDouble(parameterName: String, x: Double): Unit = setParameter(parameterName, x)

  override def setBigDecimal(parameterName: String, x: java.math.BigDecimal): Unit = setParameter(parameterName, x)

  override def setString(parameterName: String, x: String): Unit = setParameter(parameterName, x)

  override def setBytes(parameterName: String, x: Array[Byte]): Unit = setParameter(parameterName, x)

  override def setDate(parameterName: String, x: java.sql.Date): Unit = setParameter(parameterName, x)

  override def setTime(parameterName: String, x: java.sql.Time): Unit = setParameter(parameterName, x)

  override def setTimestamp(parameterName: String, x: java.sql.Timestamp): Unit = setParameter(parameterName, x)

  override def setAsciiStream(parameterName: String, x: InputStream, length: Int): Unit = setParameter(parameterName, x.limitTo(length))

  override def setBinaryStream(parameterName: String, x: InputStream, length: Int): Unit = setParameter(parameterName, x.limitTo(length))

  override def setObject(parameterName: String, x: Any, targetSqlType: Int, scale: Int): Unit = {
    setParameter(parameterName, _cast(x, targetSqlType, scale))
  }

  override def setObject(parameterName: String, x: Any, targetSqlType: Int): Unit = setParameter(parameterName, _cast(x, targetSqlType))

  override def setObject(parameterName: String, x: Any): Unit = setParameter(parameterName, x)

  override def setCharacterStream(parameterName: String, reader: Reader, length: Int): Unit = {
    setParameter(parameterName, CLOB.fromReader(reader, length))
  }

  override def setDate(parameterName: String, x: java.sql.Date, cal: Calendar): Unit = setParameter(parameterName, x)

  override def setTime(parameterName: String, x: java.sql.Time, cal: Calendar): Unit = setParameter(parameterName, x)

  override def setTimestamp(parameterName: String, x: java.sql.Timestamp, cal: Calendar): Unit = setParameter(parameterName, x)

  override def setNull(parameterName: String, sqlType: Int, typeName: String): Unit = setParameter(parameterName, _null(sqlType))

  override def getString(parameterName: String): String = StringType.convert(getParameter(parameterName))

  override def getBoolean(parameterName: String): Boolean = BooleanType.convert(getParameter(parameterName))

  override def getByte(parameterName: String): Byte = Int8Type.convert(getParameter(parameterName))

  override def getShort(parameterName: String): Short = Int16Type.convert(getParameter(parameterName))

  override def getInt(parameterName: String): Int = Int32Type.convert(getParameter(parameterName))

  override def getLong(parameterName: String): Long = Int64Type.convert(getParameter(parameterName))

  override def getFloat(parameterName: String): Float = Float32Type.convert(getParameter(parameterName))

  override def getDouble(parameterName: String): Double = Float64Type.convert(getParameter(parameterName))

  override def getBytes(parameterName: String): Array[Byte] = VarBinaryType.convert(getParameter(parameterName))

  override def getDate(parameterName: String): java.sql.Date = {
    new java.sql.Date(DateTimeType.convert(getParameter(parameterName)).getTime)
  }

  override def getTime(parameterName: String): java.sql.Time = {
    new java.sql.Time(DateTimeType.convert(getParameter(parameterName)).getTime)
  }

  override def getTimestamp(parameterName: String): java.sql.Timestamp = {
    new java.sql.Timestamp(DateTimeType.convert(getParameter(parameterName)).getTime)
  }

  override def getObject(parameterName: String): Any = getParameter(parameterName)

  override def getBigDecimal(parameterName: String): java.math.BigDecimal = {
    new java.math.BigDecimal(NumericType.convert(getParameter(parameterName)).doubleValue())
  }

  override def getObject(parameterName: String, map: util.Map[String, Class[_]]): Any = getParameter(parameterName)

  override def getRef(parameterName: String): java.sql.Ref = JDBCRef(getParameter(parameterName))

  override def getBlob(parameterName: String): java.sql.Blob = BlobType.convert(getParameter(parameterName))

  override def getClob(parameterName: String): java.sql.Clob = ClobType.convert(getParameter(parameterName))

  override def getArray(parameterName: String): java.sql.Array = {
    JDBCArray(connection, getParameter(parameterName))
  }

  override def getDate(parameterName: String, cal: Calendar): java.sql.Date = {
    new java.sql.Date(DateTimeType.convert(getParameter(parameterName)).getTime)
  }

  override def getTime(parameterName: String, cal: Calendar): java.sql.Time = {
    new java.sql.Time(DateTimeType.convert(getParameter(parameterName)).getTime)
  }

  override def getTimestamp(parameterName: String, cal: Calendar): java.sql.Timestamp = {
    new java.sql.Timestamp(DateTimeType.convert(getParameter(parameterName)).getTime)
  }

  override def getURL(parameterName: String): URL = {
    new URI(StringType.convert(getParameter(parameterName))).toURL
  }

  override def getRowId(parameterIndex: Int): java.sql.RowId = {
    JDBCRowId(Int64Type.convert(getParameter(parameterIndex)))
  }

  override def getRowId(parameterName: String): java.sql.RowId = {
    JDBCRowId(Int64Type.convert(getParameter(parameterName)))
  }

  override def setRowId(parameterName: String, x: java.sql.RowId): Unit = setParameter(parameterName, x)

  override def setNString(parameterName: String, value: String): Unit = setString(parameterName, value)

  override def setNCharacterStream(parameterName: String, value: Reader, length: Long): Unit = {
    setCharacterStream(parameterName, value, length)
  }

  override def setNClob(parameterName: String, value: java.sql.NClob): Unit = setClob(parameterName, value)

  override def setClob(parameterName: String, reader: Reader, length: Long): Unit = {
    setParameter(parameterName, CLOB.fromReader(reader, length))
  }

  override def setBlob(parameterName: String, inputStream: InputStream, length: Long): Unit = {
    setParameter(parameterName, BLOB.fromInputStream(inputStream, length))
  }

  override def setNClob(parameterName: String, reader: Reader, length: Long): Unit = {
    setClob(parameterName, reader, length)
  }

  override def getNClob(parameterIndex: Int): java.sql.NClob = {
    ClobType.convert(getParameter(parameterIndex))
  }

  override def getNClob(parameterName: String): java.sql.NClob = {
    ClobType.convert(getParameter(parameterName))
  }

  override def setSQLXML(parameterName: String, xmlObject: java.sql.SQLXML): Unit = {
    setParameter(parameterName, xmlObject)
  }

  override def getSQLXML(parameterIndex: Int): java.sql.SQLXML = {
    SQLXMLType.convert(getParameter(parameterIndex))
  }

  override def getSQLXML(parameterName: String): java.sql.SQLXML = {
    SQLXMLType.convert(getParameter(parameterName))
  }

  override def getNString(parameterIndex: Int): String = getString(parameterIndex)

  override def getNString(parameterName: String): String = getString(parameterName)

  override def getNCharacterStream(parameterIndex: Int): Reader = getCharacterStream(parameterIndex)

  override def getNCharacterStream(parameterName: String): Reader = getCharacterStream(parameterName)

  override def getCharacterStream(parameterIndex: Int): Reader = getClob(parameterIndex).getCharacterStream

  override def getCharacterStream(parameterName: String): Reader = getClob(parameterName).getCharacterStream

  override def setBlob(parameterName: String, x: java.sql.Blob): Unit = setParameter(parameterName, x)

  override def setClob(parameterName: String, x: java.sql.Clob): Unit = setParameter(parameterName, x)

  override def setAsciiStream(parameterName: String, x: InputStream, length: Long): Unit = setParameter(parameterName, x)

  override def setBinaryStream(parameterName: String, x: InputStream, length: Long): Unit = setParameter(parameterName, x)

  override def setCharacterStream(parameterName: String, reader: Reader, length: Long): Unit = {
    setParameter(parameterName, reader.limitTo(length))
  }

  override def setAsciiStream(parameterName: String, x: InputStream): Unit = setParameter(parameterName, x)

  override def setBinaryStream(parameterName: String, x: InputStream): Unit = setParameter(parameterName, x)

  override def setCharacterStream(parameterName: String, reader: Reader): Unit = setParameter(parameterName, reader)

  override def setNCharacterStream(parameterName: String, value: Reader): Unit = setCharacterStream(parameterName, value)

  override def setClob(parameterName: String, reader: Reader): Unit = {
    setParameter(parameterName, CLOB.fromReader(reader))
  }

  override def setBlob(parameterName: String, inputStream: InputStream): Unit = {
    setParameter(parameterName, BLOB.fromInputStream(inputStream))
  }

  override def setNClob(parameterName: String, reader: Reader): Unit = {
    setClob(parameterName, CLOB.fromReader(reader))
  }

  override def getObject[T](parameterIndex: Int, `type`: Class[T]): T = {
    getParameter(parameterIndex).asInstanceOf[T]
  }

  override def getObject[T](parameterName: String, `type`: Class[T]): T = {
    getParameter(parameterName).asInstanceOf[T]
  }

  private def getParameter(parameterIndex: Int): Any = {
    outParameters(parameterIndex)
  }

  private def getParameter(parameterName: String): Any = {
    outParameters(parameterName)
  }

  private def setParameter(parameterName: String, value: Any): Unit = {
    outParameters(parameterName) = value
  }

}