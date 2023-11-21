package com.lollypop.runtime.conversions

/**
 * Scala Conversion - an interface to implicitly convert Lollypop instances to their Scala equivalent
 */
trait ScalaConversion {

  def toScala: Any

}
