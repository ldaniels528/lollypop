package com.qwery.util

import com.qwery.util.OptionHelper._
import org.scalatest.funspec.AnyFunSpec

class OptionHelperTest  extends AnyFunSpec {

  describe(ResourceHelper.getClass.getSimpleName) {

    it("should attempt to find a nonEmpty option: optionA ?? optionB") {
      assert(Some("Hello") ?? None contains "Hello")
      assert(None ?? Some("World") contains "World")
    }

    it("should provide a default value: Some(5) || 5") {
      assert((Some("Hello") || "World") == "Hello")
      assert((None || "World") == "World")
    }

    it("should auto-convert T to Option[T]") {
      import com.qwery.util.OptionHelper.implicits.risky.value2Option
      assert(5.nonEmpty)
    }

  }

}
