package com.lollypop.runtime.datatypes

import com.lollypop.language.LollypopUniverse
import com.lollypop.runtime.datatypes.DataTypeFunSpec.implicits.DataTypeConversion
import com.lollypop.runtime.{LollypopVM, Scope}
import com.lollypop.util.DateHelper
import com.lollypop.util.StringRenderHelper.StringRenderer

import scala.concurrent.duration.DurationInt

class DateTimeTypeTest extends DataTypeFunSpec {
  implicit val ctx: LollypopUniverse = LollypopUniverse()
  implicit val scope: Scope = Scope()

  describe(DateTimeType.getClass.getSimpleName) {

    it("should detect DateType column types") {
      verifyType(value = new java.util.Date(), expectedType = DateTimeType)
      verifyType(value = new java.sql.Date(System.currentTimeMillis()), expectedType = DateTimeType)
      verifyType(value = new java.sql.Timestamp(System.currentTimeMillis()), expectedType = DateTimeType)
    }

    it("should encode/decode Date values") {
      verifyCodec(DateTimeType, new java.util.Date())
    }

    it("should resolve 'DateTime'") {
      verifySpec(spec = "DateTime", expected = DateTimeType)
    }

    it("should conversions to DateType") {
      val dateString = "2021-08-05T04:18:30.000Z"
      assert(dateString.convertTo(DateTimeType) == DateHelper.parse(dateString))
    }

    it("should provide a SQL representation") {
      verifySQL("DateTime", DateTimeType)
    }

    it("should add an interval to a date") {
      val (_, _, results) = LollypopVM.executeSQL(Scope(), "DateTime('2021-09-02T11:22:33.000Z') + Interval('5 DAYS')")
      assert(results.renderFriendly == "2021-09-07T11:22:33.000Z")
    }

    it("should subtract a date from a date") {
      val (_, _, results) = LollypopVM.executeSQL(Scope(), "DateTime('2021-09-02T11:22:33.000Z') - DateTime('2021-09-02T10:22:33.000Z')")
      assert(results == 1.hours)
    }

    it("should subtract an interval from a date function") {
      val (_, _, results) = LollypopVM.executeSQL(Scope(), "DateTime('2021-09-02T11:22:33.000Z') - Interval('5 DAYS')")
      assert(results.renderFriendly == "2021-08-28T11:22:33.000Z")
    }

  }

}
