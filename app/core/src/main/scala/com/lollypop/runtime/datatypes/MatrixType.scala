package com.lollypop.runtime.datatypes

import com.lollypop.language.HelpDoc.{CATEGORY_TRANSFORMATION, PARADIGM_FUNCTIONAL}
import com.lollypop.language.models.{ColumnType, Expression}
import com.lollypop.language.{HelpDoc, dieArgumentMismatch, dieUnsupportedConversion}
import com.lollypop.runtime._
import com.lollypop.runtime.datatypes.Vector.Vector
import com.lollypop.runtime.devices.FieldMetadata
import com.lollypop.runtime.instructions.expressions.NamedFunctionCall

import java.nio.ByteBuffer
import java.nio.ByteBuffer.{allocate, wrap}

case class MatrixType(rows: Int, cols: Int, override val isExternal: Boolean = false)
  extends FixedLengthDataType(name = "Matrix", maxSizeInBytes = INT_BYTES * 2 + LONG_BYTES * (rows * cols))
    with FunctionalType[Matrix] {

  override def construct(args: Seq[Any]): Matrix = args match {
    case Nil => new Matrix(rows, cols)
    case _ => MatrixType.construct(args)
  }

  override def convert(value: Any): Matrix = value match {
    case Some(v) => convert(v)
    case matrix: Matrix => matrix
    case vectors: Array[Vector] => new Matrix(vectors)
    case x => dieUnsupportedConversion(x, name)
  }

  override def decode(buf: ByteBuffer): Matrix = Matrix.decode(buf)

  override def encodeValue(value: Any): Array[Byte] = {
    wrap(convert(value).encode) ~> { buf => allocate(INT_BYTES + buf.limit()).put(buf).flipMe().array() }
  }

  override def getJDBCType: Int = java.sql.Types.ARRAY

  override def maxPhysicalSize: Int = FieldMetadata.BYTES_LENGTH + INT_BYTES + maxSizeInBytes

  override def toColumnType: ColumnType = ColumnType(name = name, rows, cols)

  override def toJavaType(hasNulls: Boolean = true): Class[_] = classOf[Matrix]

  override def toSQL: String = s"$name($rows, $cols)${if (isExternal) "*" else ""}"
}

/**
 * Represents a Matrix type with a default size (256-bytes)
 */
object MatrixType extends DataTypeParser with ConstructorSupport[Matrix] {

  def apply(args: Expression*): NamedFunctionCall = NamedFunctionCall("Matrix", args.toList)

  override def construct(args: Seq[Any]): Matrix = args match {
    case Seq(rows, cols) => new Matrix(Int32Type.convert(rows), Int32Type.convert(cols))
    case x => dieArgumentMismatch(x.length, 2, 2)
  }

  override def getCompatibleType(`class`: Class[_]): Option[DataType] = `class` match {
    //case c if c == classOf[Matrix] => Some(MatrixType)
    case _ => None
  }

  override def getCompatibleValue(value: Any): Option[DataType] = value match {
    case matrix: Matrix => Some(MatrixType(matrix.rows, matrix.cols))
    case _ => None
  }

  override def help: List[HelpDoc] = {
    import com.lollypop.runtime.implicits.risky._
    List(HelpDoc(
      name = "Matrix",
      category = CATEGORY_TRANSFORMATION,
      paradigm = PARADIGM_FUNCTIONAL,
      syntax = "new Matrix([ ... ])",
      featureTitle = "Matrix and Vector Literals",
      description = "Creates a new matrix",
      example =
        """|vector = [2.0, 1.0, 3.0]
           |matrixA = new Matrix([
           |  [1.0, 2.0, 3.0],
           |  [4.0, 5.0, 6.0],
           |  [7.0, 8.0, 9.0]
           |])
           |matrixA * vector
           |""".stripMargin
    ))
  }

  override def parseDataType(columnType: ColumnType)(implicit scope: Scope): Option[DataType] = {
    columnType.name match {
      case s if synonyms.contains(s) =>
        columnType.typeArgs match {
          case args if args.length == 2 =>
            args.map(Int32Type.convert) match {
              case Seq(n, m) => Some(MatrixType(n, m))
              case args => columnType.dieArgumentMismatch(args.length, minArgs = 2, maxArgs = 2)
            }
          case args => columnType.dieArgumentMismatch(args.length, minArgs = 2, maxArgs = 2)
        }
      case _ => None
    }
  }

  override def synonyms: Set[String] = Set("Matrix")

}

