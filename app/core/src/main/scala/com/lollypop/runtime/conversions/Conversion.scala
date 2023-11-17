package com.lollypop.runtime.conversions

/**
 * Represents a Conversion from Type/Kind to another
 */
trait Conversion {

  def convert(value: Any): Any

}
