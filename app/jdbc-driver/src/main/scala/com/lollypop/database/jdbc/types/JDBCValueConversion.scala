package com.lollypop.database.jdbc.types

import com.lollypop.language.models.Expression.implicits.LifestyleExpressions
import com.lollypop.runtime.Scope
import com.lollypop.runtime.datatypes._
import com.lollypop.util.IOTools.RichReader
import com.lollypop.util.DateHelper
import com.lollypop.util.StringRenderHelper.StringRenderer

import java.io.{InputStream, Reader}
import scala.annotation.tailrec

/**
 * JDBC Value Conversion
 */
object JDBCValueConversion {

  def _cast(value: Any, targetSqlType: Int, scale: Int = 0): Option[Any] = {
    val dataType = toDataType(targetSqlType)
    Option(dataType.convert(value))
  }

  def _null(sqlType: Int): Option[Any] = None

  def toJDBCType(typeName: String): Int = DataType(typeName.ct)(Scope()).getJDBCType

  def toDataType(sqlType: Int): DataType = {
    import java.sql.Types._
    sqlType match {
      case ARRAY => ArrayType(AnyType)
      case BIGINT => Int64Type
      case BINARY | VARBINARY => VarBinaryType
      case BLOB => BlobType
      case BIT | BOOLEAN => BooleanType
      case CHAR => CharType
      case CLOB | NCLOB => ClobType
      case DATE | TIME | TIMESTAMP | TIME_WITH_TIMEZONE => DateTimeType
      case DECIMAL => NumericType
      case DOUBLE | REAL => Float64Type
      case FLOAT => Float32Type
      case INTEGER => Int32Type
      case JAVA_OBJECT => AnyType
      case OTHER => AnyType
      case ROWID => Int64Type
      case SMALLINT => Int16Type
      case SQLXML => SQLXMLType
      case STRUCT => TableType(columns = Nil)
      case TINYINT => Int8Type
      case LONGNVARCHAR | NVARCHAR | VARCHAR => StringType
      case _ => AnyType
    }
  }

  final implicit class JDBCStringRenderer(val item: Any) extends AnyVal {
    def renderAsStringOrBytes: Either[String, Array[Byte]] = {
      @tailrec def recurse(value: Any): Either[String, Array[Byte]] = {
        value match {
          // JDBC types
          case a: java.sql.Array => recurse(a.getArray)
          case b: java.sql.Blob => Right(b.getBinaryStream.readAllBytes())
          case c: java.sql.Clob => Right(c.getAsciiStream.readAllBytes())
          case r: java.sql.Ref => recurse(r.getObject)
          case r: java.sql.RowId => Right(r.getBytes)
          case s: java.sql.SQLXML => Right(s.getBinaryStream.readAllBytes())
          case s: java.sql.Struct => recurse(s.getAttributes)
          // other types
          case b: Array[Byte] => Right(b)
          case d: java.util.Date => recurse(DateHelper.formatAsTimestamp(d))
          case i: InputStream => Right(i.readAllBytes())
          case r: Reader => Right(r.mkString().getBytes())
          case s: String if s.contains('"') => Right(s.getBytes())
          case _ => Left(value.renderAsJson)
        }
      }

      recurse(item)
    }

  }

}
