package com.lollypop.language

/**
 * Integrated Help Documentation
 */
trait HelpIntegration {

  /**
   * Provides help for instruction
   * @return the [[HelpDoc help-doc]]
   */
  def help: List[HelpDoc]

}
