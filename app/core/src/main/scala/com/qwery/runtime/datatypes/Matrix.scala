package com.qwery.runtime.datatypes

import com.qwery.runtime.datatypes.Vector.Vector
import com.qwery.runtime.devices.RowCollectionZoo.createQueryResultTable
import com.qwery.runtime.devices.{RowCollection, TableColumn}
import com.qwery.runtime.errors.{MatrixDimensionMismatchError, MatrixMustBeSquareError}
import com.qwery.runtime.instructions.queryables.TableRendering
import com.qwery.runtime.{INT_BYTES, LONG_BYTES, QweryNative, Scope}
import com.qwery.util.ByteBufferHelper.DataTypeBuffer
import com.qwery.util.CodecHelper.EnrichedByteArray
import com.qwery.util.StringRenderHelper.StringRenderer
import com.qwery.{QweryException, die}
import org.apache.commons.math3.linear._
import qwery.io.{Decoder, Encodable}

import java.nio.ByteBuffer

/**
 * Represents an n x m matrix
 * @param rows the number of rows within the matrix
 * @param cols the number of columns within the matrix
 */
class Matrix(val rows: Int, val cols: Int) extends TableRendering with Encodable with QweryNative {
  require(rows > 0 && cols > 0, die("Matrix dimensions must be positive"))
  private val elements = Array.ofDim[Double](rows, cols)

  def this(matrixData: Array[Vector]) = {
    this(matrixData.length, matrixData(0).length)
    for (i <- 0 until rows) {
      require(matrixData(i).length == cols, die("All columns must be the same length"))
      for (j <- 0 until cols) {
        this (i, j) = matrixData(i)(j)
      }
    }
  }

  /**
   * Returns the results of the division of two matrices.
   * @param that the [[Matrix matrix]] to subtract
   * @return a new matrix as the result of the subtraction.
   * @throws QweryException if both matrices have the same number of rows and columns.
   */
  def /(that: Matrix): Matrix = {
    require(this.isSameDimensions(that), dieMatrixDimensionMismatchError())
    require(isSquare, dieMatrixMustBeSquareError())
    val resultMatrix = this * that.inverse
    resultMatrix
  }

  /**
   * Returns the results of the subtraction of two matrices.
   * @param that the [[Matrix matrix]] to subtract
   * @return a new matrix as the result of the subtraction.
   * @throws QweryException if both matrices have the same number of rows and columns.
   */
  def -(that: Matrix): Matrix = {
    require(this.isSameDimensions(that), dieMatrixDimensionMismatchError())
    val resultMatrix = new Matrix(rows, cols)
    for {
      i <- 0 until rows
      j <- 0 until cols
    } resultMatrix.elements(i)(j) = elements(i)(j) - that.elements(i)(j)
    resultMatrix
  }

  /**
   * Returns the results of the addition of two matrices.
   * @param that the [[Matrix matrix]] to subtract
   * @return a new matrix as the result of their subtraction.
   * @throws QweryException if both matrices have the same number of rows and columns.
   */
  def +(that: Matrix): Matrix = {
    require(this.isSameDimensions(that), dieMatrixDimensionMismatchError())
    val resultMatrix = new Matrix(rows, cols)
    for {
      i <- 0 until rows
      j <- 0 until cols
    } resultMatrix(i, j) = this (i, j) + that(i, j)
    resultMatrix
  }

  /**
   * Multiplies a matrix by a [[Vector vector]]
   * @param vector the vector to multiply
   * @return the transformed vector
   * @example {{{
   *  val v = [2.0, 1.0, 3.0]
   *  val m = new Matrix([
   *    [1.0, 2.0, 3.0],
   *    [4.0, 5.0, 6.0],
   *    [7.0, 8.0, 9.0]
   *  ])
   *  m * v
   * }}}
   */
  def *(vector: Vector): Vector = {
    require(vector.length == cols, die("Matrix and vector dimensions are not compatible for multiplication"))
    val resultVector = Array.fill(rows)(0.0)
    for {
      i <- 0 until rows
      j <- 0 until cols
    } resultVector(i) += elements(i)(j) * vector(j)
    resultVector
  }

  /**
   * Scales a matrix
   * @param factor the scale factor
   * @return a new matrix
   * @example {{{
   *  val m = (new Matrix(3,3)).identity()
   *  m * 2
   * }}}
   */
  def *(factor: Number): Matrix = {
    val n = factor.doubleValue()
    val scaledData = for {cols <- elements; col = cols.map(_ * n)} yield col
    new Matrix(scaledData)
  }

  /**
   * Scales a matrix
   * @param value the scale factor
   * @return a new matrix
   */
  def *(value: Double): Matrix = *(factor = value)

  /**
   * Scales a matrix
   * @param value the scale factor
   * @return a new matrix
   */
  def *(value: Float): Matrix = *(factor = value)

  /**
   * Scales a matrix
   * @param value the scale factor
   * @return a new matrix
   */
  def *(value: Int): Matrix = *(factor = value)

  /**
   * Scales a matrix
   * @param value the scale factor
   * @return a new matrix
   */
  def *(value: Long): Matrix = *(factor = value)

  /**
   * Multiplies two matrices
   * @param that the [[Matrix other matrix]]
   * @return a new matrix
   */
  def *(that: Matrix): Matrix = {
    require(this.cols == that.rows, die("Number of columns in first matrix must match number of rows in second matrix"))
    val result = new Matrix(this.rows, that.cols)
    for {
      i <- 0 until this.rows
      j <- 0 until that.cols
      k <- 0 until this.cols
    } result(i, j) += this (i, k) * that(k, j)
    result
  }

  /**
   * Raises a square matrix to a power (positive integer) involves multiplying the matrix by itself multiple times.
   * @param power a power/exponent (positive integer)
   * @return a new matrix
   */
  def **(power: Int): Matrix = {
    require(isSquare, dieMatrixMustBeSquareError())
    require(power >= 0, "Power must be a non-negative integer.")
    (0 until power).foldLeft(new Matrix(rows, cols).identity) { case (m, _) => m * this }
  }

  def apply(i: Int, j: Int): Double = elements(i)(j)

  def getEigenValuesAndVectors(tolerance: Double, maxIterations: Int): (Vector, Matrix) = {
    require(isSquare, dieMatrixMustBeSquareError())
    qrAlgorithm(tolerance, maxIterations)
  }

  /**
   * The QR algorithm is an iterative numerical method used to find all the eigenvalues and eigenvectors of a matrix.
   * It is particularly efficient for finding eigenvalues of large, non-symmetric matrices. In the context of this
   * explanation, we will focus on finding eigenvalues and eigenvectors for a real symmetric matrix.
   *
   * The QR algorithm works by repeatedly applying the QR decomposition with shifts to the given matrix.
   * The QR decomposition factorizes a matrix into an orthogonal matrix (Q) and an upper triangular matrix (R):
   * <p>A = Q * R</p>
   * Here, A is the original matrix, Q is an orthogonal matrix (its transpose is its inverse), and R is an upper
   * triangular matrix. Orthogonal matrices have the property that their eigenvectors are orthonormal, meaning they
   * are mutually perpendicular and have unit length.
   *
   * The basic idea of the QR algorithm is to apply the QR decomposition repeatedly on the given matrix A to obtain:
   * <p>
   * A₁ = Q₁ * R₁<br>
   * A₂ = R₁ * Q₁ = Q₂ * R₂<br>
   * A₃ = R₂ * Q₂ = Q₃ * R₃<br>
   * ...
   * </p>
   * As we perform more iterations, the matrix Aᵢ approaches an upper triangular form, and the diagonal elements of Aᵢ
   * converge to the eigenvalues of the original matrix A. Additionally, the product of all the orthogonal matrices
   * (Q₁ * Q₂ * Q₃ * ...) converges to the matrix of eigenvectors of A.
   *
   * To speed up the convergence, the QR algorithm with shifts uses a shifting technique. During each iteration, a shift
   * is applied to make the matrix closer to being upper triangular. The choice of shift can affect the convergence speed,
   * and various shift strategies can be used (e.g., Wilkinson shift, Rayleigh quotient shift).
   *
   * Once the algorithm converges (when the matrix becomes nearly upper triangular), the diagonal elements represent the
   * eigenvalues, and the orthogonal matrices' product represents the eigenvectors.
   *
   * In the implementation provided earlier, the qrAlgorithm function repeatedly applies QR decomposition with shifts
   * to the matrix matrix. The QR decomposition is performed using the qrDecomposition helper function, and the iteration
   * continues until the matrix becomes nearly upper triangular or the maximum number of iterations (maxIterations) is
   * reached. The resulting diagonal elements of the matrix represent the eigenvalues, and the orthogonal matrices'
   * product represents the eigenvectors. The isUpperTriangular function helps check if the matrix is nearly upper
   * triangular, which serves as the termination condition for the iteration.
   *
   * Overall, the QR algorithm with shifts is a powerful and widely used method for computing eigenvalues and eigenvectors,
   * especially for large matrices where direct methods may become computationally expensive.
   * @param tolerance     the error tolerance
   * @param maxIterations the maximum number of iterations
   * @return
   */
  private def qrAlgorithm(tolerance: Double, maxIterations: Int): (Vector, Matrix) = {
    // Initialize Q and R matrices
    var qMatrix = new Matrix(Array.tabulate(rows, rows)((i, j) => if (i == j) 1.0 else 0.0))
    var rMatrix = new Matrix(elements.map(_.clone))
    var iteration = 0
    while (!rMatrix.isUpperTriangular(tolerance) && iteration < maxIterations) {
      val (q, r) = rMatrix.qrDecomposition
      qMatrix = qMatrix * q
      rMatrix = r * q
      iteration += 1
    }

    val eigenvalues = rMatrix.elements.map(_(rows - 1))
    val eigenvectors = qMatrix
    (eigenvalues, eigenvectors)
  }

  // Helper function to perform a QR decomposition
  private def qrDecomposition: (Matrix, Matrix) = {
    val q = Array.tabulate(rows, rows)((i, j) => if (i == j) 1.0 else 0.0)
    val r = Array.ofDim[Double](rows, rows)

    for (k <- 0 until rows - 1; i <- k + 1 until rows) {
      val norm = math.sqrt(elements(k)(k) * elements(k)(k) + elements(i)(k) * elements(i)(k))
      val c = elements(k)(k) / norm
      val s = elements(i)(k) / norm

      // Update the k-th and i-th rows of m
      for (j <- k until rows) {
        val temp = c * elements(k)(j) + s * elements(i)(j)
        elements(i)(j) = -s * elements(k)(j) + c * elements(i)(j)
        elements(k)(j) = temp
      }

      // Update the k-th and i-th elements of r
      r(k)(k) = norm
      r(i)(k) = 0.0

      // Update the i-th column of q
      for (j <- 0 until rows) {
        val temp = c * q(k)(j) + s * q(i)(j)
        q(i)(j) = -s * q(k)(j) + c * q(i)(j)
        q(k)(j) = temp
      }
    }

    (new Matrix(q), new Matrix(r))
  }

  override def encode: Array[Byte] = {
    val buf = ByteBuffer.allocate(INT_BYTES * 2 + LONG_BYTES * (rows * cols))
    buf.putInt(rows).putInt(cols)
    for {
      i <- 0 until rows
      j <- 0 until cols
    } buf.putDouble(elements(i)(j))
    buf.flipMe().array()
  }

  override def equals(other: Any): Boolean = {
    other match {
      case that: Matrix =>
        rows == that.rows && cols == that.cols && (elements zip that.elements).forall { case (a, b) => a sameElements b }
      case _ => false
    }
  }

  override def hashCode(): Int = encode.toBase64.hashCode

  /**
   * The determinant is a scalar value that can be computed for a square matrix. It has various applications,
   * including testing invertibility.
   * @return the determinant value
   */
  def determinant: Double = {
    require(isSquare, dieMatrixMustBeSquareError())

    // Base case: for a 1x1 matrix, determinant is the single element.
    if (rows == 1) elements(0)(0)
    else {
      var det = 0.0
      for (j <- 0 until rows) {
        // Calculate the sub-matrix (excluding the current row and column).
        val subMatrix = new Matrix((for (row <- elements.indices if row != 0) yield elements(row).zipWithIndex.filter(_._2 != j).map(_._1)).toArray)
        // Calculate the sign for the current element in the expansion.
        val sign = if (j % 2 == 0) 1 else -1
        // Recursively calculate the determinant of the sub-matrix and sum the contributions.
        det += sign * elements(0)(j) * subMatrix.determinant
      }
      det
    }
  }

  /**
   * An identity matrix, often denoted as "I" or "I_n," is a special type of square matrix. It is defined as a matrix
   * with ones along the main diagonal (from the top-left to the bottom-right) and zeros in all other positions.
   * @return a new matrix
   */
  def identity: Matrix = {
    val matrix = new Matrix(rows, cols)
    for (i <- 0 until math.min(rows, cols)) matrix(i, i) = 1.0
    matrix
  }

  /**
   * Returns the inverse of the matrix
   * @return a new matrix as the result of the inversion.
   */
  def inverse: Matrix = {
    require(isSquare, dieMatrixMustBeSquareError())
    val augmentedMatrix = Array.tabulate(rows)(row => elements(row) ++ Array.tabulate(rows)(col => if (col == row) 1.0 else 0.0))
    for (pivotRow <- 0 until rows) {
      val pivotElement = augmentedMatrix(pivotRow)(pivotRow)
      for (col <- 0 until 2 * rows) {
        augmentedMatrix(pivotRow)(col) /= pivotElement
      }

      for (row <- 0 until rows) {
        if (row != pivotRow) {
          val factor = augmentedMatrix(row)(pivotRow)
          for (col <- 0 until 2 * rows) {
            augmentedMatrix(row)(col) -= factor * augmentedMatrix(pivotRow)(col)
          }
        }
      }
    }

    new Matrix(Array.tabulate(rows)(row => augmentedMatrix(row).drop(rows)))
  }

  def isSameDimensions(matrix: Matrix): Boolean = {
    rows == matrix.rows && cols == matrix.cols
  }

  /**
   * Indicates whether the matrix is square; meaning the number of rows and columns is the same.
   * @return true, the number of rows and columns is the same.
   */
  def isSquare: Boolean = rows == cols

  /**
   * An upper triangular matrix is a square matrix in which all the elements below the main diagonal (elements with row
   * index greater than column index) are zero.
   * In the QR algorithm, the isUpperTriangular method is used as a termination condition for the iteration. When the
   * matrix becomes upper triangular (or very close to it), the algorithm stops since it has found the eigenvalues and
   * eigenvectors of the input matrix.
   * @param tolerance the error tolerance
   * @return true, if the matrix is upper triangular
   * @example {{{
   *  | a  b  c  d |
   *  | 0  e  f  g |
   *  | 0  0  h  i |
   *  | 0  0  0  j |
   * }}}
   */
  def isUpperTriangular(tolerance: Double): Boolean = {
    for (i <- 1 until rows; j <- 0 until i) {
      if (math.abs(elements(i)(j)) > tolerance) {
        return false
      }
    }
    true
  }

  /**
   * Returns the square root of the matrix
   * @return a new matrix as the result of the inversion.
   */
  def sqrt: Matrix = {
    require(isSquare, dieMatrixMustBeSquareError())
    val realMatrix = new Array2DRowRealMatrix(elements)
    val eigenDecomposition = new EigenDecomposition(realMatrix)

    val eigenvalues = eigenDecomposition.getRealEigenvalues
    val eigenvectors = eigenDecomposition.getV

    // Take the square root of the eigenvalues
    val sqrtEigenvalues = eigenvalues.map(e => math.sqrt(e))

    // Form the diagonal matrix containing the square root of eigenvalues
    val sqrtD = MatrixUtils.createRealDiagonalMatrix(sqrtEigenvalues)

    // Compute the square root of the matrix using the Eigen decomposition formula
    val sqrtMatrix = eigenvectors.multiply(sqrtD).multiply(eigenvectors.transpose)
    new Matrix(sqrtMatrix.getData)
  }


  def update(i: Int, j: Int, value: Double): Unit = elements(i)(j) = value

  def rowVector(rowIndex: Int): Vector = elements(rowIndex)

  def columnVector(columnIndex: Int): Vector = elements.map(row => row(columnIndex))

  def transpose: Matrix = {
    val result = new Matrix(cols, rows)
    for {
      i <- 0 until rows
      j <- 0 until cols
    } result(j, i) = this (i, j)
    result
  }

  def toArray: Array[Vector] = elements.clone()

  override def toTable(implicit scope: Scope): RowCollection = {
    import com.qwery.runtime.devices.RecordCollectionZoo._
    val tableType = toTableType
    val rc = createQueryResultTable(tableType.columns)
    for {
      row <- 0 until rows
      columnNames = tableType.columns.map(_.name)
    } rc.insert(Map(columnNames.zipWithIndex.map { case (name, col) => name -> elements(row)(col) }: _*).toRow(rc))
    rc
  }

  override def toTableType: TableType = {
    val headers = elements.indices.map(n => String.valueOf(('A' + n).toChar)).toList
    TableType(columns = headers.map(name => TableColumn(name, `type` = Float64Type)))
  }

  override def toString: String = s"${this.getClass.getSimpleName}(${toArray.renderAsJson})"

  override def returnType: MatrixType = MatrixType(rows, cols)

  private def dieMatrixDimensionMismatchError(): Nothing = throw new MatrixDimensionMismatchError()

  private def dieMatrixMustBeSquareError() = throw new MatrixMustBeSquareError()

}

object Matrix extends Decoder[Matrix] {

  /**
   * Decodes the given byte buffer into a [[Matrix matrix]] value
   * @param buf the [[ByteBuffer byte buffer]] to decode
   * @return the decoded [[Matrix value]]
   */
  override def decode(buf: ByteBuffer): Matrix = {
    val rows = buf.getInt
    val cols = buf.getInt
    val matrix = new Matrix(rows, cols)
    for {
      i <- 0 until rows
      j <- 0 until cols
    } matrix.elements(i)(j) = buf.getDouble()
    matrix
  }

}