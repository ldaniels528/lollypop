package com.lollypop.runtime.datatypes

import com.lollypop.runtime.RuntimeFiles.RecursiveFileList
import com.lollypop.runtime.instructions.VerificationTools
import com.lollypop.runtime.{Scope, _}
import com.lollypop.util.DateHelper
import org.scalatest.funspec.AnyFunSpec

import java.io.{ByteArrayInputStream, File, FileReader}
import scala.io.Source

class BLOBTest extends AnyFunSpec with VerificationTools {
  private val baseDir = new File("app") / "jdbc-driver" / "src" / "test" / "scala" / "com" / "lollypop" / "database" / "jdbc"
  private val testFile = baseDir / "JDBCTestServer.scala"
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

  describe(classOf[BLOB].getSimpleName) {

    it("should create a BLOB from a java.io.File") {
      // create a BLOB from a file
      val blob = IBLOB(BLOB.fromFile(testFile))

      // verify its size and contents
      assert(blob.length() == testFile.length())
      val actual = blob.getBinaryStream.use(_.mkString())
      val expected = Source.fromFile(testFile).use(_.mkString).trim
      assert(actual == expected)
      blob.free()
    }

    it("should create a BLOB from a java.io.Reader") {
      val blob = IBLOB(BLOB.fromReader(new FileReader(testFile)))
      assert(blob.getBinaryStream.mkString().trim == new FileReader(testFile).mkString().trim)
      blob.free()
    }

    it("should create a BLOB from a RowCollection") {
      val (_, _, stocksBlob) =
        """|val stocks = 
           | |---------------------------------------------------------|
           | | symbol | exchange | lastSale | lastSaleTime             |
           | |---------------------------------------------------------|
           | | BXXG   | NASDAQ   |   147.63 | 2020-08-01T21:33:11.000Z |
           | | KFFQ   | NYSE     |    22.92 | 2020-08-01T21:33:11.000Z |
           | | GTKK   | NASDAQ   |   240.14 | 2020-08-07T21:33:11.000Z |
           | | KNOW   | OTCBB    |   357.21 | 2020-08-19T21:33:11.000Z |
           | |---------------------------------------------------------|
           |val stocksBlob = BLOB(stocks)
           |@stocksBlob
           |""".stripMargin.searchSQL(Scope())
      assert(stocksBlob.toMapGraph == List(
        Map("symbol" -> "BXXG", "exchange" -> "NASDAQ", "lastSale" -> 147.63, "lastSaleTime" -> DateHelper.from(1596317591000L)),
        Map("symbol" -> "KFFQ", "exchange" -> "NYSE", "lastSale" -> 22.92, "lastSaleTime" -> DateHelper.from(1596317591000L)),
        Map("symbol" -> "GTKK", "exchange" -> "NASDAQ", "lastSale" -> 240.14, "lastSaleTime" -> DateHelper.from(1596835991000L)),
        Map("symbol" -> "KNOW", "exchange" -> "OTCBB", "lastSale" -> 357.21, "lastSaleTime" -> DateHelper.from(1597872791000L))
      ))
      stocksBlob.close()
    }

    it("should determine equality between two BLOBs") {
      val blob1 = IBLOB(BLOB.fromString(xmlStockQuotes))
      val blob2 = IBLOB(BLOB.fromString(xmlStockQuotes))
      assert(blob1 == blob2)
      assert(blob1.hashCode() == blob2.hashCode())
      blob1.free()
      blob2.free()
    }

    it("should limit the transfer of bytes from a java.io.InputStream") {
      val limit = xmlStockQuotes.getBytes().length / 2
      val stream = new ByteArrayInputStream(xmlStockQuotes.getBytes())
      val blob = IBLOB(BLOB.fromInputStream(stream, limit))
      assert(blob.length() == limit)
      blob.free()
    }

    it("should search a BLOB for a pattern") {
      val blob = IBLOB(BLOB.fromFile(testFile))
      val pattern = IBLOB(BLOB.fromString("Class.forName(LollypopDriver.getClass.getName)"))
      val pos = blob.position(pattern, 0L)
      assert(pos == 296)
      blob.free()
      pattern.free()
    }

    it("should search for and replace a slice of the BLOB") {
      val blob = IBLOB(BLOB.fromFile(testFile))
      val pattern = "Class.forName(LollypopDriver.getClass.getName)".getBytes()
      val replacement = "Class.forName(DriverLollypop.getClass.getName)".getBytes()

      // search for the pattern
      val pos = blob.position(pattern, 0L)
      assert(pos == 296)

      // replace a slice of the BLOB
      blob.setBytes(pos, replacement)

      // verify the slice
      assert(blob.getBinaryStream(pos, replacement.length).mkString() == new String(replacement))
      blob.free()
    }

    it("should populate a BLOB object via setBinaryStream()") {
      val blob = IBLOB(BLOB())
      val writer = blob.setBinaryStream(0)
      writer.use(_.write(xmlStockQuotes.getBytes()))
      assert(blob.getBinaryStream.use(_.mkString()) == xmlStockQuotes)
      blob.free()
    }

    it("should populate a BLOB object via setBytes()") {
      val blob = IBLOB(BLOB())
      val bytes = xmlStockQuotes.getBytes()
      blob.setBytes(0L, bytes, 0, bytes.length)
      assert(blob.getBinaryStream.use(_.mkString()) == xmlStockQuotes)
      blob.free()
    }

    it("should truncate a BLOB") {
      val blob = IBLOB(BLOB.fromFile(testFile))
      val limit = 164
      blob.truncate(limit)
      assert(blob.length() == limit)
      assert(blob.getBinaryStream.mkString() == Source.fromFile(testFile).use(_.mkString).substring(0, limit))
      blob.free()
    }

  }

}
