package com.lollypop.language.models

import com.lollypop.language.TemplateProcessor.TokenStreamExtensions
import com.lollypop.language.models.SourceCodeInstruction.RichSourceCodeInstruction
import com.lollypop.language.{ExpressionParser, HelpDoc, SQLCompiler, TokenStream}
import com.lollypop.util.OptionHelper.OptionEnrichment

/**
 * Represents a Named Expression
 * @author lawrence.daniels@gmail.com
 */
trait NamedExpression extends Expression {

  /**
   * @return the name of the expression
   */
  def name: String

}

/**
 * Named Expression Companion
 * @author lawrence.daniels@gmail.com
 */
object NamedExpression extends ExpressionParser {

  /**
   * For pattern matching
   * @param named the given [[NamedExpression]]
   */
  def unapply(named: NamedExpression): Option[String] = named.alias ?? Some(named.name)

  override def help: List[HelpDoc] = Nil

  override def parseExpression(stream: TokenStream)(implicit compiler: SQLCompiler): Option[Expression] = {
    val _token = stream.peek
    (stream match {
      // is it a function?
      case ts if ts.isFunctionCall => FunctionCall.parseExpression(ts)
      // is it a field? (e.g. "Symbol" or "A.Symbol")?
      case ts if ts.isField => compiler.nextField(ts)
      // nothing matched
      case ts => compiler.nextExpression(ts)
    }).map(_.updateLocation(_token))
  }

  override def understands(ts: TokenStream)(implicit compiler: SQLCompiler): Boolean = {
    ts.isFunctionCall || ts.isField
  }

}