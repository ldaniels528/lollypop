package com.lollypop.runtime.conversions

import com.lollypop.language._
import com.lollypop.runtime.datatypes.{BLOB, DataTypeFunSpec}
import com.lollypop.runtime.instructions.expressions.Dictionary
import com.lollypop.runtime.{Scope, _}

import java.io.File
import scala.concurrent.duration.DurationInt
import scala.util.{Properties, Try}

class ExpressiveTypeConversionTest extends DataTypeFunSpec {
  implicit val scope: Scope = LollypopUniverse(isServerMode = true).createRootScope()

  describe(classOf[ExpressiveTypeConversion].getSimpleName) {

    it("should (true).pullBoolean") {
      assert(true.v.pullBoolean._3)
    }

    it("should (List(1)).pullBoolean") {
      assert(List(1).v.pullBoolean._3)
    }

    it("should (Some(1)).pullBoolean") {
      assert(Some(1).v.pullBoolean._3)
    }

    it("should (Try(1)).pullBoolean") {
      assert(Try(1).v.pullBoolean._3)
    }

    it("should ({name:'Dark Institute of Technology'}).pullDictionary") {
      assert(Dictionary("name" -> "Dark Institute of Technology".v).pullDictionary._3 ==
        Map("name" -> "Dark Institute of Technology"))
    }

    it("should (7.625).pullDouble") {
      assert(7.625.v.pullDouble._3 == 7.625)
    }

    it("should ('7.115').pullDouble") {
      assert("7.115".v.pullDouble._3 == 7.115)
    }

    it("should (7.0).pullDuration") {
      assert(7.0.v.pullDuration._3 == 7.millis)
    }

    it("should ('7.0').pullDuration") {
      assert("7.0".v.pullDuration._3 == 7.millis)
    }

    it("should ('.').pullFile") {
      assert(".".v.pullFile._3.getCanonicalFile == new File(".").getCanonicalFile)
    }

    it("should ('..').pullFile") {
      assert("..".v.pullFile._3.getCanonicalFile == new File("..").getCanonicalFile)
    }

    it("should ('~').pullFile") {
      assert("~".v.pullFile._3.getCanonicalFile == new File(Properties.userHome).getCanonicalFile)
    }

    it("should (7.0).pullFloat") {
      assert(7.0.v.pullFloat._3 == 7.0f)
    }

    it("should ('7.0').pullFloat") {
      assert("7.0".v.pullFloat._3 == 7.0f)
    }

    it("should ('Hello World').pullInputStream") {
      assert("Hello World".v.pullInputStream._3.mkString() == "Hello World")
    }

    it("should (BLOB('Hello World')).pullInputStream") {
      assert(BLOB.fromString("Hello World").v.pullInputStream._3.mkString() == "Hello World")
    }

    it("should (7).pullInt") {
      assert(7.v.pullInt._3 == 7)
    }

    it("should ('7').pullInt") {
      assert("7".v.pullInt._3 == 7)
    }

    it("should fail to ('7.625').pullInt") {
      assertThrows[NumberFormatException]("7.625".v.pullInt._3)
    }

    it("should ('1234567890').pullNumber") {
      assert("1234567890".v.pullNumber._3 == (1234567890L: Number))
    }

    it("should (123.775).pullString") {
      assert(123.775.v.pullString._3 == "123.775")
    }

    it("should (DateTime('2023-11-14T22:42:05.884Z')).pullString") {
      assert("DateTime".fx("2023-11-14T22:42:05.884Z".v).pullString._3 == "2023-11-14T22:42:05.884Z")
    }

  }

}
