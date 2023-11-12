package com.lollypop.runtime.datatypes

import com.lollypop.language.LollypopUniverse
import com.lollypop.runtime.Scope
import com.lollypop.runtime.datatypes.DataTypeFunSpec.implicits.DataTypeConversion

import scala.concurrent.duration.DurationInt

/**
 * IntervalType Tests
 */
class DurationTypeTest extends DataTypeFunSpec {
  implicit val ctx: LollypopUniverse = LollypopUniverse()
  implicit val scope: Scope = Scope()

  describe(DurationType.getClass.getSimpleName) {

    it("should encode/decode IntervalType values") {
      verifyCodec(DurationType, value = 12.hours)
    }

    it("should conversions to IntervalType") {
      assert("5 minutes".convertTo(DurationType) == 5.minutes)
    }

    it("should resolve 'Duration'") {
      verifySpec(spec = "Duration", expected = DurationType)
    }

    it("should resolve 'Duration[1982]'") {
      verifySpec(spec = "Duration[1982]", expected = ArrayType(DurationType, capacity = Some(1982)))
    }

    it("should provide a SQL representation") {
      verifySQL("Duration", DurationType)
    }

  }

}
