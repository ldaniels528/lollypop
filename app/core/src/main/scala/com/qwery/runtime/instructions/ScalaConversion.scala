package com.qwery.runtime.instructions

/**
 * Scala Conversion - an interface to implicitly convert Qwery instances to their Scala equivalent
 */
trait ScalaConversion {

  def toScala: Any

}
