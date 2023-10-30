package com.lollypop.database.jdbc.types

import com.lollypop.runtime._
import com.lollypop.util.ByteBufferHelper.{DataTypeBuffer, DataTypeByteBuffer}

import java.nio.ByteBuffer

/**
 * Lollypop JDBC Row ID
 * @param __id the unique row ID
 */
case class JDBCRowId(__id: ROWID) extends java.sql.RowId {
  override def getBytes: Array[Byte] = {
    ByteBuffer.allocate(ROW_ID_BYTES).putRowID(__id).flipMe().array()
  }
}
