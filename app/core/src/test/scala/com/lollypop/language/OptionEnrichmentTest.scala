package com.lollypop.language

import org.scalatest.funspec.AnyFunSpec

class OptionEnrichmentTest  extends AnyFunSpec {

  describe(classOf[OptionEnrichment[_]].getSimpleName) {

    it("should attempt to find a nonEmpty option: optionA ?? optionB") {
      assert(Some("Hello") ?? None contains "Hello")
      assert(None ?? Some("World") contains "World")
    }

    it("should provide a default value: Some(5) || 5") {
      assert((Some("Hello") || "World") == "Hello")
      assert((None || "World") == "World")
    }

    it("should auto-convert T to Option[T]") {
      import com.lollypop.runtime.implicits.risky._
      assert(5.nonEmpty)
    }

  }

}
