package com.lollypop.runtime.datatypes

import com.lollypop.LollypopException
import com.lollypop.runtime._
import com.lollypop.runtime.datatypes.Vector.Vector
import com.lollypop.runtime.errors.{MatrixDimensionMismatchError, MatrixMustBeSquareError}
import org.scalatest.funspec.AnyFunSpec

import java.nio.ByteBuffer

class MatrixTest extends AnyFunSpec {

  describe(classOf[Matrix].getSimpleName) {

    it("should throw an exception if the dimensions are not positive") {
      assertThrows[LollypopException] {
        new Matrix(0, 2)
      }
      assertThrows[LollypopException] {
        new Matrix(2, -1)
      }
    }

    it("should allow accessing and modifying elements directly") {
      val matrix = new Matrix(2, 2)
      matrix(0, 0) = 1.0
      matrix(0, 1) = 2.0
      matrix(1, 0) = 3.0
      matrix(1, 1) = 4.0
      assert(matrix(0, 0) == 1.0)
      assert(matrix(0, 1) == 2.0)
      assert(matrix(1, 0) == 3.0)
      assert(matrix(1, 1) == 4.0)
    }

    it("should return its type") {
      val matrix = new Matrix(rows = 4, cols = 3)
      assert(matrix.returnType == MatrixType(rows = 4, cols = 3))
    }

    it("should return an array of arrays describe the contents of the matrix") {
      val m1 = new Matrix(3, 3).identity.toArray
      val m2 = Array(
        Vector(1.0, 0.0, 0.0),
        Vector(0.0, 1.0, 0.0),
        Vector(0.0, 0.0, 1.0)
      )
      assert((m1 zip m2).forall { case (a, b) => a sameElements b })
    }


    it("should return the rows of a matrix") {
      val matrix = new Matrix(Array(
        Vector(1.0, 2.0, 3.0),
        Vector(4.0, 5.0, 6.0)
      ))
      assert(matrix.rowVector(0) sameElements Vector(1.0, 2.0, 3.0))
      assert(matrix.rowVector(1) sameElements Vector(4.0, 5.0, 6.0))
    }

    it("should return the columns of a matrix") {
      val matrix = new Matrix(Array(
        Vector(1.0, 2.0, 3.0),
        Vector(4.0, 5.0, 6.0)
      ))
      assert(matrix.columnVector(0) sameElements Vector(1.0, 4.0))
      assert(matrix.columnVector(1) sameElements Vector(2.0, 5.0))
      assert(matrix.columnVector(2) sameElements Vector(3.0, 6.0))
    }

    it("should compute the determinant of a square matrix") {
      val matrix = new Matrix(Array(
        Vector(1.0, 2.0, 3.0),
        Vector(4.0, 5.0, 6.0),
        Vector(7.0, 8.0, 9.0)
      ))
      assert(matrix.determinant == 0.0)
    }

    it("should compute the Eigen values and vectors of a square matrix") {
      val matrix = new Matrix(Array(
        Vector(1.0, 2.0, 3.0),
        Vector(2.0, 5.0, 6.0),
        Vector(3.0, 6.0, 9.0)
      ))
      val (values, vectors) = matrix.getEigenValuesAndVectors(tolerance = 1e-6, maxIterations = 1000)
      assert(values sameElements Vector(0.0, 0.0, 0.0))
      assert(vectors == new Matrix(Array(
        Vector(0.31051112541558024, 0.5106130610707155, 0.8017837257372732),
        Vector(-0.09862401503519716, 0.8562303700030038, -0.5070925528371099),
        Vector(-0.9454396568007303, 0.07838274904922155, 0.3162277660168383)
      )))
    }

    it("should support an identity transform (square matrix)") {
      val matrix = new Matrix(3, 3).identity
      assert(matrix == new Matrix(Array(
        Vector(1.0, 0.0, 0.0),
        Vector(0.0, 1.0, 0.0),
        Vector(0.0, 0.0, 1.0)
      )))
    }

    it("should support an identity transform (non-square matrix)") {
      val matrix = new Matrix(4, 3).identity
      assert(matrix == new Matrix(Array(
        Vector(1.0, 0.0, 0.0),
        Vector(0.0, 1.0, 0.0),
        Vector(0.0, 0.0, 1.0),
        Vector(0.0, 0.0, 0.0)
      )))
    }

    it("should support an inverse transform") {
      val matrix = new Matrix(Array(
        Vector(4.0, 0.0, 0.0),
        Vector(0.0, 8.0, 0.0),
        Vector(0.0, 0.0, 16.0)
      )).inverse
      assert(matrix == new Matrix(Array(
        Vector(0.25, 0.0, 0.0),
        Vector(0.0, 0.125, 0.0),
        Vector(0.0, 0.0, 0.0625)
      )))
    }

    it("should support a transpose transform") {
      val matrixA = new Matrix(Array(
        Vector(1.0, 2.0, 3.0),
        Vector(4.0, 5.0, 6.0)
      ))
      assert(matrixA.rows == 2)
      assert(matrixA.cols == 3)

      val matrixB = matrixA.transpose
      assert(matrixB.rows == 3)
      assert(matrixB.cols == 2)

      assert(matrixB == new Matrix(Array(
        Vector(1.0, 4.0),
        Vector(2.0, 5.0),
        Vector(3.0, 6.0)
      )))
    }

    it("should support addition") {
      val (_, _, result) = LollypopVM.executeSQL(Scope(),
        """|val matrixA = new Matrix([
           |  [4.0, 7.0],
           |  [2.0, 9.0],
           |])
           |val matrixB = new Matrix([
           |  [1.0, 3.0],
           |  [5.0, 2.0],
           |])
           |matrixA + matrixB
           |""".stripMargin)
      assert(result == new Matrix(Array(
        Vector(5.0, 10.0),
        Vector(7.0, 11.0)
      )))
    }

    it("should support division") {
      val (_, _, result) = LollypopVM.executeSQL(Scope(),
        """|val matrixA = new Matrix([
           |  [4.0, 7.0],
           |  [2.0, 9.0],
           |])
           |val matrixB = new Matrix([
           |  [1.0, 3.0],
           |  [5.0, 2.0],
           |])
           |matrixA / matrixB
           |""".stripMargin)
      assert(result == new Matrix(Array(
        Vector(2.0769230769230766, 0.3846153846153846),
        Vector(3.1538461538461537, -0.23076923076923073)
      )))
    }

    it("should not support division with dissimilar dimensions") {
      assertThrows[MatrixDimensionMismatchError] {
        val matrixA = new Matrix(Array(
          Vector(4.0, 7.0),
          Vector(2.0, 9.0)
        ))
        val matrixB = new Matrix(Array(
          Vector(1.0, 3.0, 6.0),
          Vector(5.0, 2.0, 7.0)
        ))
        matrixA / matrixB
      }
    }

    it("should support subtraction") {
      val (_, _, result) = LollypopVM.executeSQL(Scope(),
        """|val matrixA = new Matrix([
           |  [4.0, 7.0],
           |  [2.0, 9.0],
           |])
           |val matrixB = new Matrix([
           |  [1.0, 3.0],
           |  [5.0, 2.0],
           |])
           |matrixA - matrixB
           |""".stripMargin)
      assert(result == new Matrix(Array(
        Vector(3.0, 4.0),
        Vector(-3.0, 7.0)
      )))
    }

    it("should support multiplication by a number (Double)") {
      val (_, _, result) = LollypopVM.executeSQL(Scope(),
        """|val matrixA = (new Matrix(3,3)).identity()
           |matrixA * 2.0
           |""".stripMargin)
      assert(result == new Matrix(Array(
        Vector(2.0, 0.0, 0.0),
        Vector(0.0, 2.0, 0.0),
        Vector(0.0, 0.0, 2.0)
      )))
    }

    it("should support multiplication by a number (Float)") {
      val (_, _, result) = LollypopVM.executeSQL(Scope(),
        """|val matrixA = (new Matrix(3,3)).identity()
           |matrixA * 2f
           |""".stripMargin)
      assert(result == new Matrix(Array(
        Vector(2.0, 0.0, 0.0),
        Vector(0.0, 2.0, 0.0),
        Vector(0.0, 0.0, 2.0)
      )))
    }

    it("should support multiplication by a number (Int)") {
      val (_, _, result) = LollypopVM.executeSQL(Scope(),
        """|val matrixA = (new Matrix(3,3)).identity()
           |matrixA * 2
           |""".stripMargin)
      assert(result == new Matrix(Array(
        Vector(2.0, 0.0, 0.0),
        Vector(0.0, 2.0, 0.0),
        Vector(0.0, 0.0, 2.0)
      )))
    }

    it("should support multiplication by a number (Long)") {
      val (_, _, result) = LollypopVM.executeSQL(Scope(),
        """|val matrixA = (new Matrix(3, 3)).identity()
           |matrixA * 2L
           |""".stripMargin)
      assert(result == new Matrix(Array(
        Vector(2.0, 0.0, 0.0),
        Vector(0.0, 2.0, 0.0),
        Vector(0.0, 0.0, 2.0)
      )))
    }

    it("should support multiplication by a vector") {
      val (_, _, result) = LollypopVM.executeSQL(Scope(),
        """|val vector = [2.0, 1.0, 3.0]
           |val matrixA = new Matrix([
           |  [1.0, 2.0, 3.0],
           |  [4.0, 5.0, 6.0],
           |  [7.0, 8.0, 9.0]
           |])
           |matrixA * vector
           |""".stripMargin)
      assert(result match {
        case vector: Vector => vector sameElements Vector(13.0, 31.0, 49.0)
        case _ => false
      })
    }

    it("should support multiplication by a matrix") {
      val (_, _, result) = LollypopVM.executeSQL(Scope(),
        """|val matrixA = (new Matrix(3, 3)).identity()
           |val matrixB = (new Matrix(3, 3)).identity()
           |matrixA * matrixB
           |""".stripMargin)
      assert(result == (new Matrix(3, 3).identity * new Matrix(3, 3).identity))
    }

    it("should support raising matrix by an exponent") {
      val (_, _, result) = LollypopVM.executeSQL(Scope(),
        """|val matrixA = new Matrix([
           |  [1.0, 2.0],
           |  [3.0, 4.0]
           |])
           |val power = 3
           |matrixA ** power
           |""".stripMargin)
      assert(result == new Matrix(Array(
        Vector(37.0, 54.0),
        Vector(81.0, 118.0)
      )))
    }

    it("should support computing the sqrt of a matrix") {
      val matrix = new Matrix(Array(
        Vector(125.0, 4.0),
        Vector(16.0, 125.0)
      ))
      val result = matrix.sqrt
      assert(result == new Matrix(Array(
        Vector(5.686716839681649, -2.147383603626662),
        Vector(-2.147383603626662, 22.746867358726597)
      )))
    }

    it("should not support raising matrix by an exponent for non-square matrices") {
      assertThrows[MatrixMustBeSquareError] {
        val matrix = new Matrix(Array(
          Vector(1.0, 2.0),
          Vector(3.0, 4.0),
          Vector(5.0, 6.0)
        ))
        val power = 3
        matrix ** power
      }
    }

    it("should encode itself as a byte array") {
      val m = new Matrix(2, 2).identity
      assert(m.encode.toBase64 == "AAAAAgAAAAI/8AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA/8AAAAAAAAA==")
    }

    it("should decode itself from a byte array") {
      val m = new Matrix(2, 2).identity
      val n = Matrix.decode(ByteBuffer.wrap(m.encode))
      assert(n == m)
      assert(n.hashCode() == m.hashCode())
    }

    it("should return a String representation of the matrix") {
      val matrix = new Matrix(Array(
        Vector(1.0, 2.0),
        Vector(3.0, 4.0)
      ))
      assert(matrix.toString == "Matrix([[1.0, 2.0], [3.0, 4.0]])")
    }

    it("should return a Table representation of the matrix") {
      implicit val scope: Scope = Scope()
      val matrix = new Matrix(Array(
        Vector(1.0, 2.0),
        Vector(3.0, 4.0)
      ))
      assert(matrix.toTable.tabulate().mkString("\n") ==
        """||-----------|
           || A   | B   |
           ||-----------|
           || 1.0 | 2.0 |
           || 3.0 | 4.0 |
           ||-----------|""".stripMargin('|'))
    }

  }

}