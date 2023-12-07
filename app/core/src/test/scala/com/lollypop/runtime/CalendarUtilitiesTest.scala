package com.lollypop.runtime

import com.lollypop.util.DateHelper
import org.scalatest.funspec.AnyFunSpec

class CalendarUtilitiesTest extends AnyFunSpec {
  private val testDate = DateHelper("2023-08-12T19:11:01.899Z")

  describe(classOf[CalendarUtilities.type].getSimpleName) {

    it("should extract the day of month") {
      assert(testDate.dayOfMonth == 12)
    }

    it("should extract the day of week") {
      assert(testDate.dayOfWeek == 7)
    }

    it("should extract the hour") {
      assert(testDate.hour == 19)
    }

    it("should extract the millisecond") {
      assert(testDate.millisecond == 899)
    }

    it("should extract the minute") {
      assert(testDate.minute == 11)
    }

    it("should extract the month") {
      assert(testDate.month == 8)
    }

    it("should extract the second") {
      assert(testDate.second == 1)
    }

    it("should split a java.util.Date into its date and time components") {
      val (date, time) = testDate.split
      assert(date.toString == "2023-08-12")
      assert(time.toString == "19:11:01.899")
    }

    it("should convert a java.util.Date into a java.sql.Timestamp") {
      assert(testDate.toTimestamp == new java.sql.Timestamp(testDate.getTime))
    }

    it("should extract the year") {
      assert(testDate.year == 2023)
    }

  }

}
