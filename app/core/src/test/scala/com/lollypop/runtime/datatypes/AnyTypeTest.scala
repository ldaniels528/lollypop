package com.lollypop.runtime.datatypes

import com.lollypop.language.{LollypopUniverse, _}
import com.lollypop.runtime.datatypes.DataTypeFunSpec.FakeNews
import com.lollypop.runtime.instructions.VerificationTools
import com.lollypop.runtime.{DatabaseObjectRef, LollypopVM, Scope}
import com.lollypop.util.DateHelper
import lollypop.lang.BitArray

import java.nio.ByteBuffer
import java.sql.Timestamp
import java.time.LocalDateTime

/**
 * AnyType Tests
 */
class AnyTypeTest extends DataTypeFunSpec with VerificationTools {
  implicit val ctx: LollypopUniverse = LollypopUniverse()
  implicit val scope: Scope = Scope()

  describe(classOf[AnyType].getSimpleName) {

    it("should detect InstanceType column types") {
      verifyType(value = FakeNews(message = "Hello World"), expectedType = AnyType("com.lollypop.runtime.datatypes.DataTypeFunSpec$FakeNews"))
    }

    it("should encode/decode InstanceType values") {
      verifyCodec(AnyType("com.lollypop.runtime.datatypes.DataTypeTest$FakeNews"), value = FakeNews("Hello World"))
    }

    it("should provide a SQL representation") {
      verifySQL("`java.io.File`", AnyType("java.io.File"))
    }

    it("should resolve '`java.util.Date`'") {
      verifySpec(spec = "`java.util.Date`", expected = DateTimeType)
    }

    it("should resolve 'java.util.UUID[5]'") {
      verifySpec(spec = "`java.util.UUID`[5]", expected = ArrayType(UUIDType, capacity = Some(5)))
    }

    it("should resolve classOf[BitArray] as BitArrayType") {
      assert(AnyType.parseDataType(classOf[BitArray].getName.ct).map(_.name) contains BitArrayType.name)
    }

    it("should resolve classOf[LocalDateTime] as DateTimeType") {
      assert(AnyType.parseDataType(classOf[LocalDateTime].getName.ct) contains DateTimeType)
    }

    it("should resolve classOf[Timestamp] as DateTimeType") {
      assert(AnyType.parseDataType(classOf[Timestamp].getName.ct) contains DateTimeType)
    }

    it("should be encode/decode an object") {
      val expect = new java.math.BigDecimal(67.11)
      val bytes = AnyType.encode(expect)
      val actual = AnyType.decode(ByteBuffer.wrap(bytes))
      assert(actual == expect)
    }

    it("should allow an object of a subtype") {
      val dataType = AnyType(classOf[Number].getName)
      assert(dataType.convert(123) == 123)
      assert(dataType.convert(123.0) == 123.0)
    }

    it("should serialize/deserialize objects within a table") {
      val ref = DatabaseObjectRef(getTestTableName)
      val (scope, cost, _) = LollypopVM.executeSQL(Scope(),
        s"""|drop if exists $ref
            |create table $ref (
            |  symbol: String(6),
            |  exchange: String(6),
            |  lastSale `java.math.BigDecimal`,
            |  lastSaleTime: DateTime
            |) &&
            |insert into $ref (symbol, exchange, lastSale, lastSaleTime)
            |values ('AAPL', 'NYSE',   new `java.math.BigDecimal`(67.11), DateTime('2022-09-04T23:36:47.846Z')),
            |       ('AMD',  'NASDAQ', new `java.math.BigDecimal`(98.76), DateTime('2022-09-04T23:36:47.975Z')),
            |       ('YHOO', 'NYSE',   new `java.math.BigDecimal`(23.89), DateTime('2022-09-04T23:36:47.979Z'))
            |""".stripMargin)
      assert(cost.created == 1 & cost.inserted == 3)
      val device = scope.getRowCollection(ref)
      assert(device.toMapGraph == List(
        Map("symbol" -> "AAPL", "exchange" -> "NYSE", "lastSale" -> new java.math.BigDecimal(67.11), "lastSaleTime" -> DateHelper("2022-09-04T23:36:47.846Z")),
        Map("symbol" -> "AMD", "exchange" -> "NASDAQ", "lastSale" -> new java.math.BigDecimal(98.76), "lastSaleTime" -> DateHelper("2022-09-04T23:36:47.975Z")),
        Map("symbol" -> "YHOO", "exchange" -> "NYSE", "lastSale" -> new java.math.BigDecimal(23.89), "lastSaleTime" -> DateHelper("2022-09-04T23:36:47.979Z"))
      ))
    }

    it("should serialize/deserialize objects within a table without specifying fields") {
      val ref = DatabaseObjectRef(getTestTableName)
      val (scope, cost, _) = LollypopVM.executeSQL(Scope(),
        s"""|import "java.math.BigDecimal"
            |drop if exists $ref
            |create table $ref (
            |  symbol: String(6),
            |  exchange: String(6),
            |  lastSale `java.math.BigDecimal`,
            |  lastSaleTime: DateTime
            |) &&
            |insert into $ref
            |values ('AAPL', 'NYSE',   new BigDecimal(67.11), DateTime('2022-09-04T23:36:47.846Z')),
            |       ('AMD',  'NASDAQ', new BigDecimal(98.76), DateTime('2022-09-04T23:36:47.975Z')),
            |       ('YHOO', 'NYSE',   new BigDecimal(23.89), DateTime('2022-09-04T23:36:47.979Z'))
            |""".stripMargin)
      assert(cost.created == 1 & cost.inserted == 3)
      val device = scope.getRowCollection(ref)
      assert(device.toMapGraph == List(
        Map("symbol" -> "AAPL", "exchange" -> "NYSE", "lastSale" -> new java.math.BigDecimal(67.11), "lastSaleTime" -> DateHelper("2022-09-04T23:36:47.846Z")),
        Map("symbol" -> "AMD", "exchange" -> "NASDAQ", "lastSale" -> new java.math.BigDecimal(98.76), "lastSaleTime" -> DateHelper("2022-09-04T23:36:47.975Z")),
        Map("symbol" -> "YHOO", "exchange" -> "NYSE", "lastSale" -> new java.math.BigDecimal(23.89), "lastSaleTime" -> DateHelper("2022-09-04T23:36:47.979Z"))
      ))
    }

  }

}
