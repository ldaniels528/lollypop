package com.lollypop.util

import com.lollypop.util.StringHelper._
import org.scalatest.funspec.AnyFunSpec

class StringHelperTest extends AnyFunSpec {

  describe(StringHelper.getClass.getSimpleName) {

    it("""should evaluate: "Hello World".indexOfOpt("World")""") {
      assert("Hello World".indexOfOpt("World") contains 6)
    }

    it("""should evaluate: "Hello World".indexOfOpt("Worlds").isEmpty""") {
      assert("Hello World".indexOfOpt("Worlds").isEmpty)
    }

    it("""should evaluate: "Hello, World,".indexOfOpt(",", 7)""") {
      assert("Hello, World,".indexOfOpt(",", 7) contains 12)
    }

    it("""should evaluate: "Hello World".lastIndexOfOpt("World")""") {
      assert("Hello World".lastIndexOfOpt("World") contains 6)
    }

    it("""should evaluate: "Hello World".lastIndexOfOpt("Worlds").isEmpty""") {
      assert("Hello World".lastIndexOfOpt("Worlds").isEmpty)
    }

    it("""should evaluate: "".noneIfBlank.isEmpty""") {
      assert("".noneIfBlank.isEmpty)
    }

    it("""should evaluate: "Hello World".noneIfBlank.nonEmpty""") {
      assert("Hello World".noneIfBlank.nonEmpty)
    }

    it("""should evaluate: Some("Hello World").orBlank""") {
      assert(Some("Hello World") contains "Hello World")
    }

    it("""should evaluate: "Hello, World,".lastIndexOfOpt(",")""") {
      assert("Hello, World,".lastIndexOfOpt(",") contains 12)
    }

    it("""should evaluate: "Hello, World,".lastIndexOfOpt(",", 7)""") {
      assert("Hello, World,".lastIndexOfOpt(",", 7) contains 5)
    }

  }

}
