package com.lollypop.language

/**
 * Base class for all Language parser activities
 */
trait LanguageParser {

  /**
   * Provides help for instruction
   * @return the [[HelpDoc help-doc]]
   */
  def help: List[HelpDoc]

  /**
   * Indicates whether the next token in the stream cam be parsed
   * @param stream   the [[TokenStream token stream]]
   * @param compiler the [[SQLCompiler compiler]]
   * @return true, if the next token in the stream is parseable
   */
  def understands(stream: TokenStream)(implicit compiler: SQLCompiler): Boolean

}
