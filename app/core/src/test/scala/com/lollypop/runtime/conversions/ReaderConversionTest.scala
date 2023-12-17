package com.lollypop.runtime.conversions

import com.lollypop.LollypopException
import com.lollypop.runtime.RuntimeFiles.RecursiveFileList
import com.lollypop.runtime._
import org.scalatest.funspec.AnyFunSpec

import java.io.{File, FileInputStream, FileReader}

class ReaderConversionTest extends AnyFunSpec {
  private val srcFile = new File("app") / "core" / "src" / "test" / "resources" / "log4j.properties"
  private val srcContents =
    """|# Root logger option
       |log4j.rootLogger=INFO, stdout
       |
       |# Direct log messages to stdout
       |log4j.appender.stdout=org.apache.log4j.ConsoleAppender
       |log4j.appender.stdout.Target=System.out
       |log4j.appender.stdout.layout=org.apache.log4j.PatternLayout
       |log4j.appender.stdout.layout.ConversionPattern=%d{yyyy-MM-dd HH:mm:ss} %-5p %c{1}:%L - %m%n
       |""".stripMargin.trim

  describe(classOf[ReaderConversion].getSimpleName) {

    it("should return the contents of a File as a stream") {
      val contents = ReaderConversion.convert(srcFile).use(_.mkString().trim)
      assert(contents == srcContents)
    }

    it("should return the contents of an InputStream as a stream") {
      val contents = ReaderConversion.convert(new FileInputStream(srcFile)).use(_.mkString().trim)
      assert(contents == srcContents)
    }

    it("should return the contents of an Reader as a stream") {
      val contents = ReaderConversion.convert(new FileReader(srcFile)).use(_.mkString().trim)
      assert(contents == srcContents)
    }

    it("should return the contents of an String as a stream") {
      val contents = ReaderConversion.convert(srcContents).use(_.mkString().trim)
      assert(contents == srcContents)
    }

    it("should fail if an invalid type is passed") {
      assertThrows[LollypopException](ReaderConversion.convert(56))
    }

  }

}
