package com.lollypop.runtime.datatypes

import com.lollypop.language.LollypopUniverse
import com.lollypop.runtime.Scope
import com.lollypop.runtime.datatypes.DataTypeFunSpec.implicits.DataTypeConversion

import scala.concurrent.duration.DurationInt

/**
 * IntervalType Tests
 */
class IntervalTypeTest extends DataTypeFunSpec {
  implicit val ctx: LollypopUniverse = LollypopUniverse()
  implicit val scope: Scope = Scope()

  describe(IntervalType.getClass.getSimpleName) {

    it("should encode/decode IntervalType values") {
      verifyCodec(IntervalType, value = 12.hours)
    }

    it("should conversions to IntervalType") {
      assert("5 minutes".convertTo(IntervalType) == 5.minutes)
    }

    it("should resolve 'Interval'") {
      verifySpec(spec = "Interval", expected = IntervalType)
    }

    it("should resolve 'Interval[1982]'") {
      verifySpec(spec = "Interval[1982]", expected = ArrayType(IntervalType, capacity = Some(1982)))
    }

    it("should provide a SQL representation") {
      verifySQL("Interval", IntervalType)
    }

  }

}
