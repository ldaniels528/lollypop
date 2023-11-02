package com.lollypop.runtime.instructions.jvm

import com.lollypop.language.HelpDoc.{CATEGORY_SCOPE_SESSION, PARADIGM_OBJECT_ORIENTED}
import com.lollypop.language.models.Expression.implicits.AsFunctionArguments
import com.lollypop.language.models.{Atom, Expression, ParameterLike}
import com.lollypop.language.{HelpDoc, InvokableParser, SQLCompiler, SQLTemplateParams, TokenStream}
import com.lollypop.runtime.instructions.invocables.RuntimeInvokable
import com.lollypop.runtime.plastics.Plastic
import com.lollypop.runtime.{LollypopVM, Scope}
import com.lollypop.util.OptionHelper.OptionEnrichment
import lollypop.io.IOCost
import lollypop.lang.Null

/**
 * Class declaration
 * @param fields the class [[ParameterLike fields]]
 * @example {{{
 * class Stocks(symbol: String, exchange: String, lastSale: Double, lastSaleTime: DateTime)
 * }}}
 * @author lawrence.daniels@gmail.com
 */
case class DeclareClass(className: Atom, fields: List[ParameterLike]) extends RuntimeInvokable {

  override def execute()(implicit scope: Scope): (Scope, IOCost, Any) = {
    (scope.withVariable(className.name, value = this, isReadOnly = true), IOCost.empty, ())
  }

  def newInstance(args: Expression)(implicit scope: Scope): Plastic = {
    // create the construct arguments
    val _args = args.asArguments
    val nameValues = fields.zipWithIndex map { case (field, n) =>
      if (n < _args.size) field.name -> _args(n) else field.name -> (field.defaultValue || Null())
    }
    // return the new scope and constructed object
    val (names, ops) = (nameValues.map(_._1), nameValues.map(_._2))
    val values = LollypopVM.transform(scope, ops)._3
    Plastic(this, names, values)
  }

  override def toSQL: String = List("class", " ", className.toSQL, fields.map(_.toSQL).mkString("(", ", ", ")")).mkString

}

object DeclareClass extends InvokableParser {
  private val templateCard = "class %a:name ( ?%P:fields )"

  override def parseInvokable(ts: TokenStream)(implicit compiler: SQLCompiler): Option[DeclareClass] = {
    val params = SQLTemplateParams(ts, templateCard)
    Some(DeclareClass(className = params.atoms("name"), fields = params.parameters("fields")))
  }

  override def help: List[HelpDoc] = {
    import com.lollypop.util.OptionHelper.implicits.risky._
    List(HelpDoc(
      name = "class",
      category = CATEGORY_SCOPE_SESSION,
      paradigm = PARADIGM_OBJECT_ORIENTED,
      featureTitle = "Define and Instantiate JVM classes",
      description = "Creates a new ephemeral (in-memory) JVM-compatible class",
      syntax = templateCard,
      example =
        """|class StockQuote(symbol: String, exchange: String, lastSale: Double, lastSaleTime: Date)
           |stock = new StockQuote("ABC", "OTCBB", 0.0231, DateTime())
           |stock.toString()
           |""".stripMargin
    ))
  }

  override def understands(ts: TokenStream)(implicit compiler: SQLCompiler): Boolean = ts is "class"

}


