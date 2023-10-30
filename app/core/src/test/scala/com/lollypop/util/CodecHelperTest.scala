package com.lollypop.util

import com.lollypop.util.CodecHelper._
import com.lollypop.util.ResourceHelper._
import org.scalatest.funspec.AnyFunSpec

import java.io.{FileInputStream, FileReader}
import java.util.Base64

class CodecHelperTest extends AnyFunSpec {

  describe(classOf[CodecHelper.type].getSimpleName) {

    it("should compress and decompress data via GZIP") {
      val expected = "Hello!!World!!" * 100
      val compressed = compressGZIP(expected.getBytes)
      val actual = new String(decompressGZIP(compressed))
      info(s"expected:   ${expected.take(32)}... [${expected.length}]")
      info(s"compressed: ${new String(Base64.getEncoder.encode(compressed).take(32))}... [${compressed.length}]")
      info(s"actual:     ${actual.take(32)}... [${actual.length}]")
      assert(expected == actual)
    }

    it("should compress and decompress data via Snappy") {
      val expected = "Hello!!World!!" * 100
      val compressed = compressSnappy(expected.getBytes)
      val actual = new String(decompressSnappy(compressed))
      info(s"expected:   ${expected.take(32)}... [${expected.length}]")
      info(s"compressed: ${new String(Base64.getEncoder.encode(compressed).take(32))}... [${compressed.length}]")
      info(s"actual:     ${actual.take(32)}... [${actual.length}]")
      assert(expected == actual)
    }

    it("should convert a byte array into a BASE64 String") {
      val contents = new FileInputStream("./app/core/src/test/resources/query01.sql").use(_.toBytes).toBase64
      assert(contents ==
        """|c2VsZWN0IFN5bWJvbCwgTmFtZSwgU2VjdG9yLCBJbmR1c3RyeSwgYFN1bW1hcnkgUXVvdGVgCmZyb20gQ3VzdG9tZXJzCndoZXJlIEluZHV
           |zdHJ5ID0gJ09pbC9HYXMgVHJhbnNtaXNzaW9uJwp1bmlvbgpzZWxlY3QgU3ltYm9sLCBOYW1lLCBTZWN0b3IsIEluZHVzdHJ5LCBgU3VtbWF
           |yeSBRdW90ZWAKZnJvbSBDdXN0b21lcnMKd2hlcmUgSW5kdXN0cnkgPSAnQ29tcHV0ZXIgTWFudWZhY3R1cmluZyc=
           |""".stripMargin.replaceAll("\n", "").trim)
    }

    it("should convert the contents of a FileInputStream into a BASE64 String") {
      val contents = new FileInputStream("./app/core/src/test/resources/query01.sql").use(_.toBase64)
      assert(contents ==
        """|c2VsZWN0IFN5bWJvbCwgTmFtZSwgU2VjdG9yLCBJbmR1c3RyeSwgYFN1bW1hcnkgUXVvdGVgCmZyb20gQ3VzdG9tZXJzCndoZXJlIEluZHV
           |zdHJ5ID0gJ09pbC9HYXMgVHJhbnNtaXNzaW9uJwp1bmlvbgpzZWxlY3QgU3ltYm9sLCBOYW1lLCBTZWN0b3IsIEluZHVzdHJ5LCBgU3VtbWF
           |yeSBRdW90ZWAKZnJvbSBDdXN0b21lcnMKd2hlcmUgSW5kdXN0cnkgPSAnQ29tcHV0ZXIgTWFudWZhY3R1cmluZyc=
           |""".stripMargin.replaceAll("\n", "").trim)
    }

    it("should convert the contents of a FileInputStream into a String") {
      val contents = new FileInputStream("./app/core/src/test/resources/query01.sql").use(_.mkString())
      assert(contents ==
        """|select Symbol, Name, Sector, Industry, `Summary Quote`
           |from Customers
           |where Industry = 'Oil/Gas Transmission'
           |union
           |select Symbol, Name, Sector, Industry, `Summary Quote`
           |from Customers
           |where Industry = 'Computer Manufacturing'""".stripMargin)
    }

    it("should convert the contents of a Reader into a BASE64 String") { // 55% / 48% / 63%
      val contents = new FileReader("./app/core/src/test/resources/query01.sql").use(_.toBase64)
      assert(contents ==
        """|c2VsZWN0IFN5bWJvbCwgTmFtZSwgU2VjdG9yLCBJbmR1c3RyeSwgYFN1bW1hcnkgUXVvdGVgCmZyb20gQ3VzdG9tZXJzCndoZXJlIEluZHV
           |zdHJ5ID0gJ09pbC9HYXMgVHJhbnNtaXNzaW9uJwp1bmlvbgpzZWxlY3QgU3ltYm9sLCBOYW1lLCBTZWN0b3IsIEluZHVzdHJ5LCBgU3VtbWF
           |yeSBRdW90ZWAKZnJvbSBDdXN0b21lcnMKd2hlcmUgSW5kdXN0cnkgPSAnQ29tcHV0ZXIgTWFudWZhY3R1cmluZyc=
           |""".stripMargin.replaceAll("\n", "").trim)
    }

    it("should convert the contents of a Reader into a String") {
      val contents = new FileReader("./app/core/src/test/resources/query01.sql").use(_.mkString())
      assert(contents ==
        """|select Symbol, Name, Sector, Industry, `Summary Quote`
           |from Customers
           |where Industry = 'Oil/Gas Transmission'
           |union
           |select Symbol, Name, Sector, Industry, `Summary Quote`
           |from Customers
           |where Industry = 'Computer Manufacturing'""".stripMargin)
    }

  }

}
