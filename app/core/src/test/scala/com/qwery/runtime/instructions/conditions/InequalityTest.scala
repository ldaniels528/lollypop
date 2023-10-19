package com.qwery.runtime.instructions.conditions

import com.qwery.language.models.Inequality
import com.qwery.runtime.instructions.conditions.RuntimeInequality.OptionComparator
import com.qwery.util.DateHelper
import com.qwery.util.OptionHelper.implicits.risky.value2Option
import org.scalatest.funspec.AnyFunSpec

import java.util.UUID

class InequalityTest extends AnyFunSpec {

  describe(classOf[Inequality].getSimpleName) {

    it("should compare Date: n0 < n1") {
      val n0: Option[Any] = DateHelper("2021-08-05T04:18:30.000Z")
      val n1: Option[Any] = DateHelper("2021-08-05T04:18:31.000Z")
      assert(n0 < n1)
    }

    it("should compare Date: n0 <= n1") {
      val n0: Option[Any] = DateHelper("2021-08-05T04:18:30.000Z")
      val n1: Option[Any] = DateHelper("2021-08-05T04:18:35.000Z")
      assert(n0 <= n1)
    }

    it("should compare Date: n0 > n1") {
      val n0: Option[Any] = DateHelper("2021-08-05T04:18:31.000Z")
      val n1: Option[Any] = DateHelper("2021-08-05T04:18:30.000Z")
      assert(n0 > n1)
    }

    it("should compare Date: n0 >= n1") {
      val n0: Option[Any] = DateHelper("2021-08-05T04:18:35.000Z")
      val n1: Option[Any] = DateHelper("2021-08-05T04:18:30.000Z")
      assert(n0 >= n1)
    }

    it("should compare Number: n0 < n1") {
      val n0: Option[Any] = Some(5)
      val n1: Option[Any] = Some(7L)
      assert(n0 < n1)
    }

    it("should compare Number: n0 <= n1") {
      val n0: Option[Any] = Some(-1)
      val n1: Option[Any] = Some(7.0)
      assert(n0 <= n1)
    }

    it("should compare Number: n0 > n1") {
      val n0: Option[Any] = Some(DateHelper.now)
      val n1: Option[Any] = Some(70000L)
      assert(n0 > n1)
    }

    it("should compare Number: n0 >= n1") {
      val n0: Option[Any] = Some(DateHelper.now)
      val n1: Option[Any] = Some(6)
      assert(n0 >= n1)
    }

    it("should compare String: n0 < n1") {
      val n0: Option[Any] = "goodbye"
      val n1: Option[Any] = "hello"
      assert(n0 < n1)
    }

    it("should compare String: n0 <= n1") {
      val n0: Option[Any] = "apple"
      val n1: Option[Any] = "banana"
      assert(n0 <= n1)
    }

    it("should compare String: n0 > n1") {
      val n0: Option[Any] = "zebra"
      val n1: Option[Any] = "monkey"
      assert(n0 > n1)
    }

    it("should compare String: n0 >= n1") {
      val n0: Option[Any] = "dog"
      val n1: Option[Any] = "cat"
      assert(n0 >= n1)
    }

    it("should compare UUID: n0 < n1") {
      val n0: Option[Any] = UUID.fromString("7ca26d2e-e133-478c-9921-2126761d9339")
      val n1: Option[Any] = UUID.fromString("7ca26d2e-e133-478c-9921-2126761d9340")
      assert(n0 < n1)
    }

    it("should compare UUID: n0 <= n1") {
      val n0: Option[Any] = UUID.fromString("7ca26d2e-e133-478c-9921-2126761d9339")
      val n1: Option[Any] = UUID.fromString("7ca26d2e-e133-478c-9921-2126761d9339")
      assert(n0 <= n1)
    }

    it("should compare UUID: n0 > n1") {
      val n0: Option[Any] = UUID.fromString("7ca26d2e-e133-478c-9921-2126761d9340")
      val n1: Option[Any] = UUID.fromString("7ca26d2e-e133-478c-9921-2126761d9339")
      assert(n0 > n1)
    }

    it("should compare UUID: n0 >= n1") {
      val n0: Option[Any] = UUID.fromString("7ca26d2e-e133-478c-9921-2126761d9339")
      val n1: Option[Any] = UUID.fromString("7ca26d2e-e133-478c-9921-2126761d9339")
      assert(n0 >= n1)
    }

  }

}
