package com.qwery.runtime.instructions.queryables

import com.qwery.language.models.Expression.implicits.LifestyleExpressionsAny
import com.qwery.runtime.instructions.VerificationTools
import com.qwery.runtime.instructions.expressions.ArrayFromRange
import com.qwery.runtime.{QweryCompiler, QweryVM, Scope}
import com.qwery.util.DateHelper
import org.scalatest.funspec.AnyFunSpec
import org.slf4j.LoggerFactory

class ExplodeTest extends AnyFunSpec with VerificationTools {
  private val logger = LoggerFactory.getLogger(getClass)

  describe(classOf[Explode].getSimpleName) {

    it("should compile SQL to a model") {
      val model = QweryCompiler().compile("explode(items: [1 to 5])")
      assert(model == Explode(ArrayFromRange.Inclusive(1.v, 5.v).as("items")))
    }

    it("should decompile a model to SQL") {
      val model = Explode(ArrayFromRange.Inclusive(1.v, 5.v).as("items"))
      assert(model.toSQL == "explode([1 to 5])")
    }

    it("should explode an Array") {
      val (_, _, device) = QweryVM.searchSQL(Scope(),
        """|explode(items: [1 to 5])
           |""".stripMargin)
      device.tabulate().foreach(logger.info)
      assert(device.toMapGraph == List(Map("items" -> 1), Map("items" -> 2), Map("items" -> 3), Map("items" -> 4), Map("items" -> 5)))
    }

    it("should explode a Row") {
      val (_, _, device) = QweryVM.searchSQL(Scope(),
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
           |explode(@@rows[0])
           |""".stripMargin)
      device.tabulate().foreach(logger.info)
      assert(device.toMapGraph == List(
        Map("key" -> "symbol", "value" -> "XYZ"),
        Map("key" -> "exchange", "value" -> "AMEX"),
        Map("key" -> "lastSale", "value" -> 31.95)
      ))
    }

    it("should explode a Product instance") {
      val (_, _, device) = QweryVM.searchSQL(Scope(),
        """|set @quote = new `com.qwery.runtime.instructions.queryables.ExplodeTest$ProductStockQuote`(
           |    "ABC",
           |    "OTCBB",
           |    0.0231,
           |    DateTime(1592215200000)
           |)
           |explode(@quote)
           |""".stripMargin)
      device.tabulate().foreach(logger.info)
      assert(device.toMapGraph == List(
        Map("key" -> "symbol", "value" -> "ABC"),
        Map("key" -> "exchange", "value" -> "OTCBB"),
        Map("key" -> "lastSale", "value" -> 0.0231),
        Map("key" -> "lastSaleTime", "value" -> DateHelper("2020-06-15T10:00:00.000Z"))
      ))
    }

    it("should explode a POJO") {
      val (_, _, device) = QweryVM.searchSQL(Scope(),
        """|set @quote = new `com.qwery.runtime.instructions.queryables.ExplodeTest$PojoStockQuote`(
           |    "ABC",
           |    "OTCBB",
           |    0.0231,
           |    DateTime(1592215200000)
           |)
           |explode(@quote)
           |""".stripMargin)
      device.tabulate().foreach(logger.info)
      assert(device.toMapGraph == List(
        Map("key" -> "_class", "value" -> Class.forName("com.qwery.runtime.instructions.queryables.ExplodeTest$PojoStockQuote")),
        Map("key" -> "exchange", "value" -> "OTCBB"),
        Map("key" -> "symbol", "value" -> "ABC"),
        Map("key" -> "lastSale", "value" -> 0.0231),
        Map("key" -> "lastSaleTime", "value" -> DateHelper("2020-06-15T10:00:00.000Z"))
      ))
    }

  }

}

object ExplodeTest {

  import java.util.Date
  import scala.beans.BeanProperty

  case class ProductStockQuote(symbol: String, exchange: String, lastSale: Double, lastSaleTime: Date)

  class PojoStockQuote(@BeanProperty var symbol: String,
                       @BeanProperty var exchange: String,
                       @BeanProperty var lastSale: Double,
                       @BeanProperty var lastSaleTime: Date)

}