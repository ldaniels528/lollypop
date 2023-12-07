package com.lollypop.runtime.conversions

import com.lollypop.LollypopException
import com.lollypop.runtime._
import org.scalatest.funspec.AnyFunSpec

import java.io.{File, FileOutputStream, FileWriter}

class WriterConversionTest extends AnyFunSpec {
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

  describe(classOf[WriterConversion].getSimpleName) {

    it("should return a File as an output stream") {
      val dstFile = File.createTempFile("writer-cnv", String.valueOf(System.currentTimeMillis()))
      WriterConversion.convert(dstFile).use(_.write(srcContents))
      val contents = ReaderConversion.convert(dstFile).use(_.mkString().trim)
      assert(contents == srcContents)
    }

    it("should return the contents of an OutputStream as an output stream") {
      val dstFile = File.createTempFile("writer-cnv", String.valueOf(System.currentTimeMillis()))
      WriterConversion.convert(new FileOutputStream(dstFile)).use(_.write(srcContents))
      val contents = ReaderConversion.convert(dstFile).use(_.mkString().trim)
      assert(contents == srcContents)
    }

    it("should return the contents of an Writer as an output stream") {
      val dstFile = File.createTempFile("writer-cnv", String.valueOf(System.currentTimeMillis()))
      WriterConversion.convert(new FileWriter(dstFile)).use(_.write(srcContents))
      val contents = ReaderConversion.convert(dstFile).use(_.mkString().trim)
      assert(contents == srcContents)
    }

    it("should fail if an invalid type is passed") {
      assertThrows[LollypopException](WriterConversion.convert(56))
    }

  }

}
