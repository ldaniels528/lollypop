package com.qwery.database.jdbc.types

import com.qwery.runtime._
import com.qwery.util.ByteBufferHelper.{DataTypeBuffer, DataTypeByteBuffer}

import java.nio.ByteBuffer

/**
 * Qwery JDBC Row ID
 * @param __id the unique row ID
 */
case class JDBCRowId(__id: ROWID) extends java.sql.RowId {
  override def getBytes: Array[Byte] = {
    ByteBuffer.allocate(ROW_ID_BYTES).putRowID(__id).flipMe().array()
  }
}
