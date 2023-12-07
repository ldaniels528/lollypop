package com.lollypop.runtime

import com.lollypop.util.DateHelper
import org.scalatest.funspec.AnyFunSpec

import scala.concurrent.duration.DurationInt

class DateMathematicsTest extends AnyFunSpec {

  describe(classOf[DateMathematics].getSimpleName) {

    it("should add an Duration to a Date") {
      val dateA = DateHelper("2022-09-04T22:36:47.846Z")
      val interval = 1.hour
      val dateB = dateA + interval
      assert(dateB == DateHelper("2022-09-04T23:36:47.846Z"))
    }

    it("should subtract an Duration from a Date") {
      val dateA = DateHelper("2022-09-04T23:36:47.846Z")
      val interval = 1.hour
      val dateB = dateA - interval
      assert(dateB == DateHelper("2022-09-04T22:36:47.846Z"))
    }

    it("should subtract two Date instances") {
      val dateA = DateHelper("2022-09-04T22:36:47.846Z")
      val dateB = DateHelper("2022-09-04T23:36:47.846Z")
      val interval = dateB - dateA
      assert(interval == 1.hour)
    }

  }

}
