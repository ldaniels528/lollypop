package com.lollypop.database
package jdbc

import com.lollypop.database.jdbc.types.JDBCValueConversion._
import com.lollypop.runtime.conversions.TransferTools.{RichInputStream, RichReader}

import java.io.{InputStream, Reader}
import java.net.URL
import java.util.Calendar
import scala.beans.BeanProperty

/**
 * Lollypop JDBC Prepared Statement
 * @param connection the [[JDBCConnection connection]]
 * @param sql        the SQL query
 */
class JDBCPreparedStatement(@BeanProperty connection: JDBCConnection, sql: String)
  extends JDBCStatement(connection) with java.sql.PreparedStatement {
  private var batches: List[List[Any]] = Nil

  @BeanProperty val parameterMetaData = new JDBCParameterMetaData()

  override def addBatch(): Unit = batches = parameterMetaData.getParameters :: batches

  override def clearBatch(): Unit = batches = Nil

  override def clearParameters(): Unit = parameterMetaData.clear()

  override def execute(): Boolean = {
    val outcome = invokeQuery(sql, parameterMetaData.getParameters)
    resultSet = JDBCResultSet(connection, outcome)
    updateCount = outcome.getUpdateCount
    outcome.rows.nonEmpty
  }

  override def executeBatch(): Array[Int] = {
    val outcome = (batches.reverse map { params => invokeQuery(sql, params).getUpdateCount }).toArray
    clearBatch()
    outcome
  }

  override def executeQuery(): java.sql.ResultSet = {
    val outcome = invokeQuery(sql, parameterMetaData.getParameters)
    resultSet = JDBCResultSet(connection, outcome)
    updateCount = outcome.getUpdateCount
    resultSet
  }

  override def executeUpdate(): Int = {
    val outcome = invokeQuery(sql, parameterMetaData.getParameters)
    resultSet = JDBCResultSet(connection, outcome)
    updateCount = outcome.getUpdateCount
    updateCount
  }

  override def setNull(parameterIndex: Int, sqlType: Int): Unit = parameterMetaData(parameterIndex) = _null(sqlType)

  override def setBoolean(parameterIndex: Int, x: Boolean): Unit = parameterMetaData(parameterIndex) = x

  override def setByte(parameterIndex: Int, x: Byte): Unit = parameterMetaData(parameterIndex) = x

  override def setShort(parameterIndex: Int, x: Short): Unit = parameterMetaData(parameterIndex) = x

  override def setInt(parameterIndex: Int, x: Int): Unit = parameterMetaData(parameterIndex) = x

  override def setLong(parameterIndex: Int, x: Long): Unit = parameterMetaData(parameterIndex) = x

  override def setFloat(parameterIndex: Int, x: Float): Unit = parameterMetaData(parameterIndex) = x

  override def setDouble(parameterIndex: Int, x: Double): Unit = parameterMetaData(parameterIndex) = x

  override def setBigDecimal(parameterIndex: Int, x: java.math.BigDecimal): Unit = parameterMetaData(parameterIndex) = x

  override def setString(parameterIndex: Int, x: String): Unit = parameterMetaData(parameterIndex) = x

  override def setBytes(parameterIndex: Int, x: Array[Byte]): Unit = parameterMetaData(parameterIndex) = x

  override def setDate(parameterIndex: Int, x: java.sql.Date): Unit = parameterMetaData(parameterIndex) = x

  override def setTime(parameterIndex: Int, x: java.sql.Time): Unit = parameterMetaData(parameterIndex) = x

  override def setTimestamp(parameterIndex: Int, x: java.sql.Timestamp): Unit = parameterMetaData(parameterIndex) = x

  override def setAsciiStream(parameterIndex: Int, x: InputStream, length: Int): Unit = parameterMetaData(parameterIndex) = x.limitTo(length)

  override def setUnicodeStream(parameterIndex: Int, x: InputStream, length: Int): Unit = parameterMetaData(parameterIndex) = x.limitTo(length)

  override def setBinaryStream(parameterIndex: Int, x: InputStream, length: Int): Unit = parameterMetaData(parameterIndex) = x.limitTo(length)

  override def setObject(parameterIndex: Int, x: Any, targetSqlType: Int): Unit = parameterMetaData(parameterIndex) = _cast(x, targetSqlType)

  override def setObject(parameterIndex: Int, x: Any): Unit = parameterMetaData(parameterIndex) = x

  override def setCharacterStream(parameterIndex: Int, reader: Reader, length: Int): Unit = {
    parameterMetaData(parameterIndex) = reader.limitTo(length)
  }

  override def setRef(parameterIndex: Int, x: java.sql.Ref): Unit = parameterMetaData(parameterIndex) = x

  override def setBlob(parameterIndex: Int, x: java.sql.Blob): Unit = parameterMetaData(parameterIndex) = x

  override def setClob(parameterIndex: Int, x: java.sql.Clob): Unit = parameterMetaData(parameterIndex) = x

  override def setArray(parameterIndex: Int, x: java.sql.Array): Unit = parameterMetaData(parameterIndex) = x

  override def getMetaData: java.sql.ResultSetMetaData = Option(resultSet).map(_.getMetaData).orNull

  override def setDate(parameterIndex: Int, x: java.sql.Date, cal: Calendar): Unit = parameterMetaData(parameterIndex) = x

  override def setTime(parameterIndex: Int, x: java.sql.Time, cal: Calendar): Unit = parameterMetaData(parameterIndex) = x

  override def setTimestamp(parameterIndex: Int, x: java.sql.Timestamp, cal: Calendar): Unit = parameterMetaData(parameterIndex) = x

  override def setNull(parameterIndex: Int, sqlType: Int, typeName: String): Unit = parameterMetaData(parameterIndex) = _null(sqlType)

  override def setURL(parameterIndex: Int, x: URL): Unit = parameterMetaData(parameterIndex) = x

  override def setRowId(parameterIndex: Int, x: java.sql.RowId): Unit = parameterMetaData(parameterIndex) = x

  override def setNString(parameterIndex: Int, value: String): Unit = setString(parameterIndex, value)

  override def setNCharacterStream(parameterIndex: Int, value: Reader, length: Long): Unit = setCharacterStream(parameterIndex, value, length)

  override def setNClob(parameterIndex: Int, value: java.sql.NClob): Unit = setClob(parameterIndex, value)

  override def setClob(parameterIndex: Int, reader: Reader, length: Long): Unit = parameterMetaData(parameterIndex) = reader

  override def setBlob(parameterIndex: Int, inputStream: InputStream, length: Long): Unit = parameterMetaData(parameterIndex) = inputStream

  override def setNClob(parameterIndex: Int, reader: Reader, length: Long): Unit = setClob(parameterIndex, reader, length)

  override def setSQLXML(parameterIndex: Int, xmlObject: java.sql.SQLXML): Unit = parameterMetaData(parameterIndex) = xmlObject

  override def setObject(parameterIndex: Int, x: Any, targetSqlType: Int, scaleOrLength: Int): Unit = parameterMetaData(parameterIndex) = _cast(x, targetSqlType, scaleOrLength)

  override def setAsciiStream(parameterIndex: Int, x: InputStream, length: Long): Unit = parameterMetaData(parameterIndex) = x

  override def setBinaryStream(parameterIndex: Int, x: InputStream, length: Long): Unit = parameterMetaData(parameterIndex) = x

  override def setCharacterStream(parameterIndex: Int, reader: Reader, length: Long): Unit = parameterMetaData(parameterIndex) = reader

  override def setAsciiStream(parameterIndex: Int, x: InputStream): Unit = parameterMetaData(parameterIndex) = x

  override def setBinaryStream(parameterIndex: Int, x: InputStream): Unit = parameterMetaData(parameterIndex) = x

  override def setCharacterStream(parameterIndex: Int, reader: Reader): Unit = parameterMetaData(parameterIndex) = reader

  override def setNCharacterStream(parameterIndex: Int, value: Reader): Unit = setCharacterStream(parameterIndex, value)

  override def setClob(parameterIndex: Int, reader: Reader): Unit = parameterMetaData(parameterIndex) = reader

  override def setBlob(parameterIndex: Int, inputStream: InputStream): Unit = parameterMetaData(parameterIndex) = inputStream

  override def setNClob(parameterIndex: Int, reader: Reader): Unit = setClob(parameterIndex, reader)

  private def invokeQuery(sql: String, params: List[Any]): QueryResponse = {
    val request = JDBCQueryRequestFactory(sql, params, limit = Some(fetchSize))
    connection.client.executeJDBC(getDatabase, getSchema, request)
  }

}
