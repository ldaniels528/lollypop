package com.lollypop.runtime.instructions.queryables

import com.lollypop.language._
import com.lollypop.runtime.instructions.VerificationTools
import com.lollypop.runtime.instructions.expressions.{ArrayLiteral, Span}
import com.lollypop.runtime.{LollypopCompiler, LollypopVM, Scope}
import com.lollypop.util.DateHelper
import org.scalatest.funspec.AnyFunSpec
import org.slf4j.LoggerFactory

class TransposeTest extends AnyFunSpec with VerificationTools {
  private val logger = LoggerFactory.getLogger(getClass)

  describe(classOf[Transpose].getSimpleName) {

    it("should compile SQL to a model") {
      val model = LollypopCompiler().compile("transpose(items: [1 to 5])")
      assert(model == Transpose(ArrayLiteral(Span.Inclusive(1.v, 5.v).as("items"))))
    }

    it("should decompile a model to SQL") {
      val model = Transpose(ArrayLiteral(Span.Inclusive(1.v, 5.v).as("items")))
      assert(model.toSQL == "transpose([1 to 5])")
    }

    it("should transpose an Array") {
      val (_, _, device) = LollypopVM.searchSQL(Scope(),
        """|transpose(items: [1 to 5])
           |""".stripMargin)
      device.tabulate().foreach(logger.info)
      assert(device.toMapGraph == List(Map("items" -> 1), Map("items" -> 2), Map("items" -> 3), Map("items" -> 4), Map("items" -> 5)))
    }

    it("should transpose a Row") {
      val (_, _, device) = LollypopVM.searchSQL(Scope(),
        """|val rows = (
           |   |------------------------------|
           |   | symbol | exchange | lastSale |
           |   |------------------------------|
           |   | XYZ    | AMEX     |    31.95 |
           |   | AAXX   | NYSE     |    56.12 |
           |   | QED    | NASDAQ   |          |
           |   | JUNK   | AMEX     |    97.61 |
           |   |------------------------------|
           |)
           |transpose(@rows[0])
           |""".stripMargin)
      device.tabulate().foreach(logger.info)
      assert(device.toMapGraph == List(
        Map("name" -> "symbol", "value" -> "XYZ"),
        Map("name" -> "exchange", "value" -> "AMEX"),
        Map("name" -> "lastSale", "value" -> "31.95")
      ))
    }

    it("should transpose a Product instance") {
      val (_, _, device) = LollypopVM.searchSQL(Scope(),
        """|quote = new `com.lollypop.runtime.instructions.queryables.TransposeTest$ProductStockQuote`(
           |    "ABC",
           |    "OTCBB",
           |    0.0231,
           |    DateTime(1592215200000)
           |)
           |transpose(quote)
           |""".stripMargin)
      device.tabulate().foreach(logger.info)
      assert(device.toMapGraph == List(
        Map("key" -> "symbol", "value" -> "ABC"),
        Map("key" -> "exchange", "value" -> "OTCBB"),
        Map("key" -> "lastSale", "value" -> 0.0231),
        Map("key" -> "lastSaleTime", "value" -> DateHelper("2020-06-15T10:00:00.000Z"))
      ))
    }

    it("should transpose a POJO") {
      val (_, _, device) = LollypopVM.searchSQL(Scope(),
        """|quote = new `com.lollypop.runtime.instructions.queryables.TransposeTest$PojoStockQuote`(
           |    "ABC",
           |    "OTCBB",
           |    0.0231,
           |    DateTime(1592215200000)
           |)
           |transpose(quote)
           |""".stripMargin)
      device.tabulate().foreach(logger.info)
      assert(device.toMapGraph == List(
        Map("key" -> "_class", "value" -> Class.forName("com.lollypop.runtime.instructions.queryables.TransposeTest$PojoStockQuote")),
        Map("key" -> "exchange", "value" -> "OTCBB"),
        Map("key" -> "symbol", "value" -> "ABC"),
        Map("key" -> "lastSale", "value" -> 0.0231),
        Map("key" -> "lastSaleTime", "value" -> DateHelper("2020-06-15T10:00:00.000Z"))
      ))
    }

  }

}

object TransposeTest {

  import java.util.Date
  import scala.beans.BeanProperty

  case class ProductStockQuote(symbol: String, exchange: String, lastSale: Double, lastSaleTime: Date)

  class PojoStockQuote(@BeanProperty var symbol: String,
                       @BeanProperty var exchange: String,
                       @BeanProperty var lastSale: Double,
                       @BeanProperty var lastSaleTime: Date)

}