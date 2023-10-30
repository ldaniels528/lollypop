package lollypop.lang

import com.lollypop.runtime.LONG_BYTES
import com.lollypop.util.ByteBufferHelper.DataTypeBuffer
import lollypop.io.Encodable

import java.nio.ByteBuffer
import scala.language.implicitConversions

/**
 * Represents a Complex Number (e.g. 3i + 9)
 * @param imaginary the given imaginary value (e.g. 3i)
 * @param constant  the given constant value (e.g. 9)
 */
class ComplexNumber(val imaginary: Double, val constant: Double) extends Encodable {

  def *(number: Double): ComplexNumber = new ComplexNumber(imaginary = imaginary * number, constant = constant * number)

  def *(number: Long): ComplexNumber = *(number.doubleValue())

  def *(number: Int): ComplexNumber = *(number.doubleValue())

  def **(number: Double): ComplexNumber = {
    new ComplexNumber(imaginary = 0.0, constant = Math.pow(imaginary, number) + Math.pow(constant, number))
  }

  def **(number: Long): ComplexNumber = **(number.doubleValue())

  def **(number: Int): ComplexNumber = **(number.doubleValue())

  def /(number: Double): ComplexNumber = new ComplexNumber(imaginary = imaginary / number, constant = constant / number)

  def /(number: Long): ComplexNumber = /(number.doubleValue())

  def /(number: Int): ComplexNumber = /(number.doubleValue())

  def %(number: Double): ComplexNumber = new ComplexNumber(imaginary = imaginary % number, constant = constant % number)

  def %(number: Long): ComplexNumber = %(number.doubleValue())

  def %(number: Int): ComplexNumber = %(number.doubleValue())

  def +(number: Double): ComplexNumber = new ComplexNumber(imaginary = imaginary, constant = constant + number)

  def +(number: Long): ComplexNumber = this.+(number.doubleValue())

  def +(number: Int): ComplexNumber = this.+(number.doubleValue())

  def -(number: Double): ComplexNumber = new ComplexNumber(imaginary = imaginary, constant = constant - number)

  def -(number: Long): ComplexNumber = this.-(number.doubleValue())

  def -(number: Int): ComplexNumber = this.-(number.doubleValue())

  def +(number: ComplexNumber): ComplexNumber = new ComplexNumber(imaginary = imaginary + number.imaginary, constant = constant + number.constant)

  def -(number: ComplexNumber): ComplexNumber = new ComplexNumber(imaginary = imaginary - number.imaginary, constant = constant - number.constant)

  def *(number: ComplexNumber): ComplexNumber = new ComplexNumber(imaginary = imaginary * number.imaginary, constant = constant * number.constant)

  def **(number: ComplexNumber): ComplexNumber = {
    new ComplexNumber(imaginary = 0.0, constant = Math.pow(imaginary, number.imaginary) + Math.pow(constant, number.constant))
  }

  def /(number: ComplexNumber): ComplexNumber = new ComplexNumber(imaginary = imaginary / number.imaginary, constant = constant / number.constant)

  def %(number: ComplexNumber): ComplexNumber = new ComplexNumber(imaginary = imaginary % number.imaginary, constant = constant % number.constant)

  override def encode: Array[Byte] = {
    ByteBuffer.allocate(LONG_BYTES * 2).putDouble(imaginary).putDouble(constant).flipMe().array()
  }

  override def equals(obj: Any): Boolean = {
    obj match {
      case cn: ComplexNumber => imaginary == cn.imaginary && constant == cn.constant
      case _ => false
    }
  }

  def isImaginary: Boolean = imaginary != 0.0

  override def toString: String = {
    if (imaginary == 0.0) String.valueOf(constant)
    else if (constant == 0.0) s"${imaginary}i"
    else s"${imaginary}i ${if (constant < 0) s"- ${-constant}" else s"+ $constant"}"
  }

}

object ComplexNumber {

  def sqrt(number: Int): ComplexNumber = number match {
    case n if n < 0 => new ComplexNumber(imaginary = Math.sqrt(-n), constant = 0.0)
    case n => new ComplexNumber(imaginary = 0, constant = Math.sqrt(n))
  }

  def sqrt(number: Long): ComplexNumber = number match {
    case n if n < 0 => new ComplexNumber(imaginary = Math.sqrt(-n), constant = 0.0)
    case n => new ComplexNumber(imaginary = 0, constant = Math.sqrt(n))
  }

  def sqrt(number: Double): ComplexNumber = number match {
    case n if n < 0 => new ComplexNumber(imaginary = Math.sqrt(-n), constant = 0.0)
    case n => new ComplexNumber(imaginary = 0, constant = Math.sqrt(n))
  }

  implicit def byte2Complex(number: Byte): ComplexNumber = new ComplexNumber(imaginary = 0.0, constant = number)

  implicit def double2Complex(number: Double): ComplexNumber = new ComplexNumber(imaginary = 0.0, constant = number)

  implicit def float2Complex(number: Float): ComplexNumber = new ComplexNumber(imaginary = 0.0, constant = number)

  implicit def int2Complex(number: Int): ComplexNumber = new ComplexNumber(imaginary = 0.0, constant = number)

  implicit def long2Complex(number: Long): ComplexNumber = new ComplexNumber(imaginary = 0.0, constant = number)

  implicit def number2Complex(number: Number): ComplexNumber = new ComplexNumber(imaginary = 0.0, constant = number.doubleValue)

  implicit def short2Complex(number: Short): ComplexNumber = new ComplexNumber(imaginary = 0.0, constant = number)

  final implicit class ComplexExtension(val number: Double) extends AnyVal {
    @inline
    def i: ComplexNumber = new ComplexNumber(imaginary = number, constant = 0.0)
  }

}