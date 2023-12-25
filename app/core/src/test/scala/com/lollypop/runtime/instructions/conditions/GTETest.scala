package com.lollypop.runtime.instructions.conditions

import com.lollypop.runtime._
import com.lollypop.runtime.implicits.risky._
import com.lollypop.util.DateHelper
import org.scalatest.funspec.AnyFunSpec

import java.util.{Date, UUID}

class GTETest extends AnyFunSpec {

  describe(classOf[GTE].getSimpleName) {

    it("should compare two Date instances") {
      val a: Option[Date] = DateHelper("2021-08-05T04:18:35.000Z")
      val b: Option[Date] = DateHelper("2021-08-05T04:18:30.000Z")
      assert(a >= b)
    }

    it("should compare a Date and a Number") {
      val a: Option[Date] = DateHelper.now
      val b: Option[Int] = 6
      assert(a >= b)
    }

    it("should compare two Number instances") {
      val a: Option[Int] = 7
      val b: Option[Long] = 5L
      assert(a >= b)
    }

    it("should compare two String instances") {
      val a: Option[String] = "dog"
      val b: Option[String] = "cat"
      assert(a >= b)
    }

    it("should compare two UUID instances") {
      val a: Option[UUID] = UUID.fromString("7ca26d2e-e133-478c-9921-2126761d9339")
      val b: Option[UUID] = UUID.fromString("7ca26d2e-e133-478c-9921-2126761d9339")
      assert(a >= b)
    }

    it("should evaluate: (a >= b) is true") {
      val (_, _, isTrue) =
        """|a = 35
           |a >= 30
           |""".stripMargin.executeSQL(Scope())
      assert(isTrue == true)
    }

    it("should evaluate: (a >= b) is false") {
      val (_, _, isTrue) =
        """|a = 29
           |a >= 30
           |""".stripMargin.executeSQL(Scope())
      assert(isTrue == false)
    }

    it("should evaluate: a >= x < b") {
      val (_, _, isTrue) =
        """|x = 38
           |30 >= x < 35
           |""".stripMargin.executeSQL(Scope())
      assert(isTrue == false)
    }

    it("should evaluate: a >= x <= b") {
      val (_, _, isTrue) =
        """|x = 3
           |1 >= x <= 5
           |""".stripMargin.executeSQL(Scope())
      assert(isTrue == true)
    }

  }

}
