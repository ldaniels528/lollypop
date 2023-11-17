package com.lollypop.runtime.datatypes

import com.lollypop.AppConstants
import com.lollypop.runtime.DatabaseObjectNS
import com.lollypop.runtime.datatypes.SQLXML.SYSTEM_ID
import com.lollypop.runtime.devices.RowCollectionZoo._
import com.lollypop.util.ResourceHelper._
import com.lollypop.runtime.conversions.TransferTools.{RichInputStream, RichReader}
import org.apache.commons.io.IOUtils
import org.xml.sax.InputSource
import org.xml.sax.ext.DefaultHandler2
import org.xml.sax.helpers.{DefaultHandler, XMLReaderFactory}

import java.io._
import java.net.URL
import java.nio.charset.Charset
import javax.xml.parsers.DocumentBuilderFactory
import javax.xml.transform.dom.{DOMResult, DOMSource}
import javax.xml.transform.sax.{SAXResult, SAXSource}
import javax.xml.transform.{Result, Source}

/**
 * Lollypop-native JDBC-compatible SQL/XML implementation
 * @param ns  the [[DatabaseObjectNS namespace]]
 * @param raf the [[RandomAccessFile]]
 * @example {{{
 *   SQLXML('Hello World')
 * }}}
 */
class SQLXML(val ns: DatabaseObjectNS, val raf: RandomAccessFile) extends ISQLXML with AbstractLOB {

  override def equals(obj: Any): Boolean = {
    obj match {
      case that: java.sql.SQLXML => this.getBinaryStream.mkString() == that.getBinaryStream.mkString()
      case _ => super.equals(obj)
    }
  }

  override def hashCode(): Int = toString.hashCode()

  override def getBinaryStream: InputStream = createInputStream()

  override def setBinaryStream(): OutputStream = createOutputStream(pos = 0L)

  override def getCharacterStream: Reader = new InputStreamReader(getBinaryStream)

  override def setCharacterStream(): Writer = new OutputStreamWriter(setBinaryStream())

  override def getString: String = getBinaryStream.mkString()

  override def setString(value: String): Unit = {
    raf.setLength(0)
    raf.write(value.getBytes("utf-8"))
  }

  override def getSource[T <: Source](sourceClass: Class[T]): T = {
    val source = sourceClass.getConstructor().newInstance()
    source.setSystemId(SYSTEM_ID)
    Option(source) collect {
      case saxSource: SAXSource =>
        saxSource.setInputSource(new InputSource(getBinaryStream))
        saxSource.setXMLReader(XMLReaderFactory.createXMLReader())
      case domSource: DOMSource =>
        val factory = DocumentBuilderFactory.newInstance()
        val builder = factory.newDocumentBuilder()
        domSource.setNode(builder.parse(getBinaryStream))
    }
    source
  }

  override def setResult[T <: Result](resultClass: Class[T]): T = {
    val result = resultClass.getConstructor().newInstance()
    result.setSystemId(SYSTEM_ID)
    Option(result) collect {
      case saxResult: SAXResult =>
        saxResult.setHandler(new DefaultHandler())
        saxResult.setLexicalHandler(new DefaultHandler2())
      case domResult: DOMResult =>
        val factory = DocumentBuilderFactory.newInstance()
        val builder = factory.newDocumentBuilder()
        domResult.setNode(builder.parse(getBinaryStream))
    }
    result
  }

  override def toString: String = s"${getClass.getSimpleName}(\"${getCharacterStream.mkString()}\")"

}

/**
 * Lollypop SQL/XML Companion
 */
object SQLXML {
  private val SYSTEM_ID = s"Lollypop_v${AppConstants.version}"

  def apply(): ISQLXML = {
    val ns = createTempNS()
    ns.createRoot()
    val sqlXML: ISQLXML = new SQLXML(ns, new RandomAccessFile(ns.sqlXmlFile, "rw"))
    sqlXML
  }

  /**
   * Creates a new SQLXML from a byte array
   * @param bytes the byte array source
   * @return a [[ISQLXML SQLXML]]
   */
  def fromBytes(bytes: Array[Byte]): ISQLXML = {
    val sqlXml = SQLXML()
    sqlXml.setBinaryStream().use(_.write(bytes))
    sqlXml
  }

  /**
   * Creates a new SQLXML from a character array source
   * @param chars the character array source
   * @return a [[ISQLXML SQLXML]]
   */
  def fromChars(chars: Array[Char]): ISQLXML = {
    val clob = SQLXML()
    clob.setCharacterStream().use(_.write(chars))
    clob
  }

  /**
   * Creates a new SQLXML from a file source
   * @param file the [[File file]] source
   * @return a [[ISQLXML SQLXML]]
   */
  def fromFile(file: File): ISQLXML = new FileInputStream(file).use(fromInputStream)

  /**
   * Creates a new SQLXML from a file source
   * @param in the [[InputStream input stream]] source
   * @return a [[ISQLXML SQLXML]]
   */
  def fromInputStream(in: InputStream): ISQLXML = {
    val sqlXml = SQLXML()
    sqlXml.setBinaryStream().use(IOUtils.copy(in, _))
    sqlXml
  }

  /**
   * Creates a new SQLXML from a [[Reader]] source
   * @param reader the [[Reader reader]] source
   * @return a [[ISQLXML SQLXML]]
   */
  def fromReader(reader: Reader): ISQLXML = {
    val sqlXml = SQLXML()
    sqlXml.setBinaryStream().use(IOUtils.copy(reader, _, Charset.defaultCharset()))
    sqlXml
  }

  /**
   * Creates a new SQLXML from a string source
   * @param string the [[String string]] source
   * @return a [[ISQLXML SQLXML]]
   */
  def fromString(string: String): ISQLXML = fromBytes(string.getBytes())

  /**
   * Creates a new SQLXML from a URL source
   * @param url the [[URL]] source
   * @return a [[ISQLXML SQLXML]]
   */
  def fromURL(url: URL): ISQLXML = url.openStream().use(fromInputStream)

}