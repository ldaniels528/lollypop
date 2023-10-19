package com.qwery.runtime

/**
 *  Represents a Database Object
 *  @author lawrence.daniels@gmail.com
 */
trait DataObject {

  /**
   * @return the [[DatabaseObjectNS database object namespace]]
   */
  def ns: DatabaseObjectNS

}
