package com.lollypop.database.jdbc.types

import com.lollypop.database.jdbc.JDBCTestServer
import com.lollypop.runtime.RuntimeFiles.RecursiveFileList
import com.lollypop.runtime.datatypes.{ISQLXML, SQLXML}
import com.lollypop.util.IOTools.{RichInputStream, RichReader}
import com.lollypop.util.ResourceHelper._
import org.apache.commons.io.IOUtils
import org.scalatest.funspec.AnyFunSpec
import org.xml.sax.Attributes
import org.xml.sax.helpers.DefaultHandler

import java.io.{BufferedReader, File, FileReader, InputStreamReader}
import java.net.URL
import java.sql.DriverManager
import javax.xml.parsers.SAXParserFactory
import javax.xml.transform.TransformerFactory
import javax.xml.transform.dom.{DOMResult, DOMSource}
import javax.xml.transform.sax.{SAXResult, SAXSource}
import scala.collection.concurrent.TrieMap

class SQLXMLTest extends AnyFunSpec with JDBCTestServer {
  private val testFile = new File("app") / "jdbc-driver" / "src" / "test" / "scala" / "com" / "lollypop" / "database" / "jdbc" / "JDBCTestServer.scala"
  private val xmlStockQuotes =
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

  describe(classOf[SQLXML].getSimpleName) {

    it("should create an SQLXML object from a byte array") {
      val sqlXml = ISQLXML(SQLXML.fromBytes(xmlStockQuotes.getBytes()))
      assert(sqlXml.getString == xmlStockQuotes)
      sqlXml.free()
    }

    it("should create an SQLXML object from a File") {
      val sqlXml = ISQLXML(SQLXML.fromFile(new File("app") / "jdbc-driver" / "src" / "test" / "resources" / "stock_quotes.xml"))
      assert(sqlXml.getString == xmlStockQuotes)
      sqlXml.free()
    }

    it("should create a SQLXML from a java.io.Reader") {
      val sqlXml = ISQLXML(SQLXML.fromReader(new FileReader(testFile)))
      assert(sqlXml.getBinaryStream.mkString().trim == new FileReader(testFile).mkString().trim)
      sqlXml.free()
    }

    it("should create an SQLXML object from a String") {
      val sqlXml = ISQLXML(SQLXML.fromString(xmlStockQuotes))
      assert(sqlXml.getString == xmlStockQuotes)
      sqlXml.free()
    }

    it("should create an SQLXML object from a URL") {
      val sqlXml = ISQLXML(SQLXML.fromURL(getResource("/stock_quotes.xml")))
      assert(sqlXml.getString == xmlStockQuotes)
      sqlXml.free()
    }

    it("should determine equality between two SQLXMLs") {
      val sqlXml1 = ISQLXML(SQLXML.fromString(xmlStockQuotes))
      val sqlXml2 = ISQLXML(SQLXML.fromString(xmlStockQuotes))
      assert(sqlXml1 == sqlXml2)
      assert(sqlXml1.hashCode() == sqlXml2.hashCode())
      sqlXml1.free()
      sqlXml2.free()
    }

    it("should create and populate an SQLXML object via getString() and setString()") {
      DriverManager.getConnection(jdbcURL) use { conn =>
        val sqlXml = ISQLXML(conn.createSQLXML())
        sqlXml.setString(getResource("/stock_quotes.xml").openStream().mkString())
        assert(sqlXml.getString == xmlStockQuotes)
        sqlXml.free()
      }
    }

    it("should create and populate an SQLXML object via getCharacterStream() and setCharacterStream()") {
      DriverManager.getConnection(jdbcURL) use { conn =>
        // create and populate an SQL-XML object
        val sqlXml = ISQLXML(conn.createSQLXML())
        sqlXml.setCharacterStream() use { writer =>
          new BufferedReader(new InputStreamReader(getResource("/stock_quotes.xml").openStream()))
            .use(IOUtils.copy(_, writer))
        }

        // verify the contents
        assert(sqlXml.getCharacterStream.mkString() == xmlStockQuotes)
        sqlXml.free()
      }
    }

    it("should create and populate an SQLXML object via getBinaryStream() and setBinaryStream()") {
      DriverManager.getConnection(jdbcURL) use { conn =>
        // create and populate an SQL-XML object
        val sqlXml = ISQLXML(conn.createSQLXML())
        sqlXml.setBinaryStream() use { out =>
          getResource("/stock_quotes.xml").openStream().use(IOUtils.copy(_, out))
        }

        // verify the contents
        assert(sqlXml.getCharacterStream.mkString() == xmlStockQuotes)
        sqlXml.free()
      }
    }

    it("should parse an SQLXML object via SAXParser") {
      // create the SQL-XML object
      val sqlXml = ISQLXML(SQLXML.fromURL(getResource("/stock_quotes.xml")))

      // parse the XML
      val saxParserFactory = SAXParserFactory.newInstance()
      val saxParser = saxParserFactory.newSAXParser()
      val handler = new StockResultHandler()
      saxParser.parse(sqlXml.getBinaryStream, handler)

      // verify the results
      assert(handler.results == List(
        Map("exchange" -> "NYSE", "symbol" -> "HPV", "lastSale" -> "12.76", "lastSaleTime" -> "2023-06-27T05:17:21.111Z"),
        Map("exchange" -> "OTCBB", "symbol" -> "ABC", "lastSale" -> "0.7654", "lastSaleTime" -> "2023-06-27T05:17:21.096Z")
      ))
      sqlXml.free()
    }

    it("should parse an SQLXML object via SAX / Transformer") {
      DriverManager.getConnection(jdbcURL) use { conn =>
        val sqlXml = ISQLXML(conn.createSQLXML())
        sqlXml.setString(xmlStockQuotes)

        // create and verify a SAXSource
        val saxSource = sqlXml.getSource(classOf[SAXSource])
        assert(Option(saxSource).nonEmpty)

        // create and verify the SAXResult
        val saxResult = sqlXml.setResult(classOf[SAXResult])
        assert(Option(saxResult).nonEmpty)

        // parse the XML
        val handler = new StockResultHandler()
        saxResult.setHandler(handler)
        val transformer = TransformerFactory.newInstance().newTransformer()
        transformer.transform(saxSource, saxResult)

        // verify the results
        assert(handler.results == List(
          Map("exchange" -> "NYSE", "symbol" -> "HPV", "lastSale" -> "12.76", "lastSaleTime" -> "2023-06-27T05:17:21.111Z"),
          Map("exchange" -> "OTCBB", "symbol" -> "ABC", "lastSale" -> "0.7654", "lastSaleTime" -> "2023-06-27T05:17:21.096Z")
        ))
        sqlXml.free()
      }
    }

    it("should parse an SQLXML object via DOM / Transformer") {
      DriverManager.getConnection(jdbcURL) use { conn =>
        val sqlXml = ISQLXML(conn.createSQLXML())
        sqlXml.setString(xmlStockQuotes)

        // create and verify a DOMSource
        val domSource = sqlXml.getSource(classOf[DOMSource])
        assert(Option(domSource).nonEmpty)

        // create and verify the DOMResult
        val domResult = sqlXml.setResult(classOf[DOMResult])
        assert(Option(domResult).nonEmpty)

        // parse the XML
        val transformer = TransformerFactory.newInstance().newTransformer()
        transformer.transform(domSource, domResult)
        sqlXml.free()
      }
    }

  }

  private def decode(attributes: Attributes): Seq[String] = {
    for (n <- 0 until attributes.getLength) yield attributes.getQName(n)
  }

  private def getResource(path: String): URL = getClass.getResource(path)

  class StockResultHandler() extends DefaultHandler {
    private var tagStack: List[String] = Nil
    private var quotes: List[Map[String, String]] = Nil
    private val mapping = TrieMap[String, String]()

    def results: List[Map[String, String]] = quotes

    override def startElement(uri: String, localName: String, qName: String, attributes: Attributes): Unit = {
      logger.info(s"startElement(uri: '$uri', localName: '$localName', qName: '$qName', attributes: ${decode(attributes).mkString("[", ", ", "]")})")
      tagStack = qName :: tagStack
    }

    override def endElement(uri: String, localName: String, qName: String): Unit = {
      logger.info(s"endElement(uri: '$uri', localName: '$localName', qName: '$qName')")
      tagStack = tagStack.tail
      qName match {
        case "stock-quote" => quotes = mapping.toMap :: quotes
        case _ =>
      }
    }

    override def characters(ch: Array[Char], start: Int, length: Int): Unit = {
      val text = String.valueOf(ch, start, length).trim
      if (text.nonEmpty) logger.info(s"characters(ch: '$text', start: $start, length: $length)")
      tagStack.headOption.foreach { tag =>
        if (text.nonEmpty) mapping.put(tag, text)
      }
    }
  }

}
