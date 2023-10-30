package com.lollypop.util

import com.lollypop.util.JSONSupport.JsValueConversion
import spray.json.JsValue

import scala.jdk.CollectionConverters.{CollectionHasAsScala, MapHasAsScala}

/**
 * JVM Support
 */
object JVMSupport {

  final implicit class NormalizeAny(val value: Any) extends AnyVal {

    @inline
    def normalize: Any = value match {
      case j: JsValue => j.unwrapJSON
      case x => x.normalizeJava
    }

    @inline
    def normalizeArrays: Any = value match {
      case a: Array[_] => a.map(_.normalizeArrays).toSeq
      case x => x.normalize
    }

    @inline
    def normalizeJava: Any = value match {
      case j: java.util.Collection[_] => j.asScala
      case j: java.util.Map[_, _] => j.asScala
      case x => x
    }
  }

}
