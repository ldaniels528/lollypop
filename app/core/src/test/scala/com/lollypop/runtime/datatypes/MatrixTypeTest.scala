package com.lollypop.runtime.datatypes

import com.lollypop.language.models.ColumnType
import com.lollypop.language.{LifestyleExpressionsAny, LollypopUniverse}
import com.lollypop.runtime.instructions.expressions.NamedFunctionCall
import com.lollypop.runtime.{LollypopVM, Scope}

import java.nio.ByteBuffer.wrap

class MatrixTypeTest extends DataTypeFunSpec {
  implicit val ctx: LollypopUniverse = LollypopUniverse()
  implicit val scope: Scope = ctx.createRootScope()

  describe(MatrixType.getClass.getSimpleName) {

    it("should compile: Matrix(3, 2)") {
      verifySpec(spec = "Matrix(3, 2)", expected = MatrixType(3, 2))
    }

    it("should decompile: Matrix(3, 2)") {
      verifySQL("Matrix(3, 2)", MatrixType(3, 2))
    }

    it("should produce the constructor function: MatrixType(4, 3)") {
      assert(MatrixType(4.v, 3.v) == NamedFunctionCall("Matrix", 4.v, 3.v))
    }

    it("should produce instances via ConstructorSupport (instanced)") {
      assert(MatrixType(4, 3).construct(Nil) == new Matrix(rows = 4, cols = 3))
    }

    it("should produce instances via ConstructorSupport (static)") {
      assert(MatrixType.construct(Seq(4, 3)) == new Matrix(rows = 4, cols = 3))
    }

    it("should convert an array of vectors into a Matrix instance") {
      val matrix = MatrixType(3, 3).convert(Array(
        Vector(1.0, 2.0),
        Vector(3.0, 4.0),
        Vector(5.0, 6.0)
      ))
      assert(matrix == new Matrix(Array(
        Vector(1.0, 2.0),
        Vector(3.0, 4.0),
        Vector(5.0, 6.0)
      )))
    }

    it("should provide maxPhysicalSize (storage requirements)") {
      val matrixType = MatrixType(3, 2)
      assert(matrixType.maxPhysicalSize == 61)
    }

    it("should encode/decode a Matrix instance") {
      val matrixType = MatrixType(3, 2)
      val matrix = new Matrix(Array(
        Vector(1.0, 2.0),
        Vector(3.0, 4.0),
        Vector(5.0, 6.0)
      ))
      val decoded = matrixType.decode(wrap(matrixType.encode(matrix)))
      assert(decoded == new Matrix(Array(
        Vector(1.0, 2.0),
        Vector(3.0, 4.0),
        Vector(5.0, 6.0)
      )))
    }

    it("should return the equivalent raw type: MatrixType(3, 2).toColumnType") {
      assert(MatrixType(3, 2).toColumnType == ColumnType("Matrix", 3, 2))
    }

    it("should return the JDBC type: MatrixType(5, 4).getJDBCType") {
      assert(MatrixType(5, 4).getJDBCType == java.sql.Types.ARRAY)
    }

    it("should return the JVM type: MatrixType(5, 4).toJavaType()") {
      assert(MatrixType(5, 4).toJavaType() == classOf[Matrix])
    }

    it("should execute: new Matrix(3, 3)") {
      val (_, _, result) = LollypopVM.executeSQL(scope,
        """|new Matrix(3, 3)
           |""".stripMargin)
      assert(result == new Matrix(3, 3))
    }

  }

}