package com.qwery.runtime.datatypes

import com.qwery.runtime.{Scope, readable}
import org.scalatest.Assertion
import org.scalatest.funspec.AnyFunSpec
import org.slf4j.{Logger, LoggerFactory}

import java.nio.ByteBuffer.wrap

trait DataTypeFunSpec extends AnyFunSpec {
  protected val logger: Logger = LoggerFactory.getLogger(getClass)

  def verifyArray[A](dataType: DataType, expected: Array[A]): Assertion = {
    val arrayType = ArrayType(dataType, capacity = Some(10))
    logger.info(s"expected: ${readable(expected)}")

    val bytes = arrayType.encode(expected)
    val actual = arrayType.decode(wrap(bytes))
    logger.info(s"actual: ${readable(actual)}")

    assert(actual.map(readable).toSeq == expected.map(readable).toSeq)
  }

  def verifyCodec[A](dataType: DataType, value: A): Assertion = {
    val value0 = dataType.encode(value)
    val value1 = dataType.decode(wrap(value0))
    logger.info(s"expected: ${readable(value)}")
    logger.info(s"actual: ${readable(value1)}")
    assert(readable(value) == readable(value))
  }

  def verifySpec(spec: String, expected: DataType)(implicit scope: Scope): Assertion = {
    logger.info(s"column type spec: $spec")
    logger.info(s"expected: $expected")

    val dataType = DataType.parse(spec)
    logger.info(s"actual: $dataType")
    assert(dataType.name == expected.name)
  }

  def verifySQL(sql: String, dataType: DataType): Assertion = {
    logger.info(s"expected: $sql")
    logger.info(s"actual: ${dataType.toSQL}")
    assertResult(sql)(dataType.toSQL)
  }

  def verifyType(value: Any, expectedType: DataType): Assertion = {
    val columnTypeA = Inferences.fromValue(value)
    val columnTypeB = Inferences.fromClass(value.getClass)
    logger.info(s"expected type: $expectedType, class detected type: $columnTypeB, value detected type: $columnTypeA")
    assert(columnTypeA.name == columnTypeB.name)
    assert(columnTypeB.name == expectedType.name)
  }

}

/**
 * DataTypeFunSpec Test Companion
 */
object DataTypeFunSpec {

  case class FakeNews(message: String) extends Serializable

  object implicits {

    /**
     * Data Type Conversion
     * @param value the given [[Any value]]
     */
    final implicit class DataTypeConversion(val value: Any) extends AnyVal {
      @inline def convertTo(newType: DataType): Any = newType.convert(value)
    }

  }

}