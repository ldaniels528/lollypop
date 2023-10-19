package com.qwery.runtime.datatypes

import com.qwery.runtime.RuntimeFiles.RecursiveFileList
import com.qwery.runtime.instructions.VerificationTools
import com.qwery.util.CodecHelper.{RichInputStream, RichReader}
import com.qwery.util.ResourceHelper._
import org.scalatest.funspec.AnyFunSpec

import java.io.{ByteArrayInputStream, File, FileReader, InputStreamReader}
import scala.io.Source

class CLOBTest extends AnyFunSpec with VerificationTools {
  private val testFile = new File("app") / "jdbc-driver" / "src" / "test" / "scala" / "com" / "qwery" / "database" / "jdbc" / "JDBCTestServer.scala"
  private val xmlStockQuotes: String =
    """|<?xml version="1.0" encoding="ISO-8859-1"?>
       |<stock-quotes>
       |    <stock-quote>
       |        <symbol>ABC</symbol>
       |        <exchange>OTCBB</exchange>
       |        <lastSale>0.7654</lastSale>
       |        <lastSaleTime>2023-06-27T05:17:21.096Z</lastSaleTime>
       |    </stock-quote>
       |    <stock-quote>
       |        <symbol>HPV</symbol>
       |        <exchange>NYSE</exchange>
       |        <lastSale>12.76</lastSale>
       |        <lastSaleTime>2023-06-27T05:17:21.111Z</lastSaleTime>
       |    </stock-quote>
       |</stock-quotes>""".stripMargin

  describe(classOf[CLOB].getSimpleName) {

    it("should create and populate a CLOB") {
      // create a CLOB from a file
      val clob = ICLOB(CLOB.fromFile(testFile))

      // verify its size and contents
      assert(clob.length() == testFile.length())
      val actual = clob.getAsciiStream.use(_.mkString())
      val expected = Source.fromFile(testFile).use(_.mkString).trim
      assert(actual == expected)
      clob.free()
    }

    it("should determine equality between two CLOBs") {
      val clob1 = ICLOB(CLOB.fromString(xmlStockQuotes))
      val clob2 = ICLOB(CLOB.fromString(xmlStockQuotes))
      assert(clob1 == clob2)
      assert(clob1.hashCode() == clob2.hashCode())
      clob1.free()
      clob2.free()
    }

    it("should create a CLOB from a byte array") {
      val clob = ICLOB(CLOB.fromBytes(xmlStockQuotes.getBytes()))
      assert(clob.getAsciiStream.mkString().trim == xmlStockQuotes.trim)
      clob.free()
    }

    it("should create a CLOB from a java.io.Reader") {
      val clob = ICLOB(CLOB.fromReader(new FileReader(testFile)))
      assert(clob.getAsciiStream.mkString().trim == new FileReader(testFile).mkString().trim)
      clob.free()
    }

    it("should limit the transfer of characters from a reader") {
      val limit = xmlStockQuotes.length / 2
      val reader = new InputStreamReader(new ByteArrayInputStream(xmlStockQuotes.getBytes()))
      val clob = ICLOB(CLOB.fromReader(reader, limit))
      assert(clob.length() == limit)
      clob.free()
    }

    it("should search a CLOB for a pattern") {
      val clob = ICLOB(CLOB.fromFile(testFile))
      val pattern = ICLOB(CLOB.fromString("Class.forName(QweryDriver.getClass.getName)"))
      val pos = clob.position(pattern, 0L)
      assert(pos == 231)
      clob.free()
      pattern.free()
    }

    it("should search for and replace a slice of the CLOB") {
      val clob = ICLOB(CLOB.fromFile(testFile))
      val pattern = "Class.forName(QweryDriver.getClass.getName)"
      val replacement = "Class.forName(DriverQwery.getClass.getName)"

      // search for the pattern
      val pos = clob.position(pattern, 0L)
      assert(pos == 231)

      // replace a slice of the CLOB
      clob.setString(pos, replacement)

      // verify the slice
      assert(clob.getCharacterStream(231, replacement.length).mkString() == new String(replacement))
      clob.free()
    }

    it("should populate a CLOB object via setAsciiStream()") {
      val clob = ICLOB(CLOB())
      val writer = clob.setAsciiStream(0)
      writer.use(_.write(xmlStockQuotes.getBytes()))
      assert(clob.getAsciiStream.use(_.mkString()) == xmlStockQuotes)
      clob.free()
    }

    it("should populate a CLOB object via setCharacterStream()") {
      val clob = ICLOB(CLOB())
      val writer = clob.setCharacterStream(0)
      writer.use(_.write(xmlStockQuotes))
      assert(clob.getCharacterStream.use(_.mkString()) == xmlStockQuotes)
      clob.free()
    }

    it("should populate a CLOB object via setString()") {
      val clob = ICLOB(CLOB())
      clob.setString(0L, xmlStockQuotes, 0, xmlStockQuotes.length)
      assert(clob.getAsciiStream.use(_.mkString()) == xmlStockQuotes)
      clob.free()
    }

    it("should truncate a CLOB") {
      val clob = ICLOB(CLOB.fromFile(testFile))
      val limit = 155
      clob.truncate(limit)
      assert(clob.length() == limit)
      assert(clob.getAsciiStream.mkString() == Source.fromFile(testFile).use(_.mkString).substring(0, limit))
      clob.free()
    }

  }

}
