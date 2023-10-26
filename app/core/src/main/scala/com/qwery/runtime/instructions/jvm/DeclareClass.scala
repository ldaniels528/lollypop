package com.qwery.runtime.instructions.jvm

import com.qwery.language.HelpDoc.{CATEGORY_SESSION, PARADIGM_OBJECT_ORIENTED}
import com.qwery.language.models.Expression.implicits.AsFunctionArguments
import com.qwery.language.models.{Atom, Expression, ParameterLike}
import com.qwery.language.{HelpDoc, InvokableParser, SQLCompiler, SQLTemplateParams, TokenStream}
import com.qwery.runtime.instructions.invocables.RuntimeInvokable
import com.qwery.runtime.plastics.Plastic
import com.qwery.runtime.{QweryVM, Scope}
import com.qwery.util.OptionHelper.OptionEnrichment
import qwery.io.IOCost
import qwery.lang.Null

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
    val values = QweryVM.transform(scope, ops)._3
    Plastic(this, names, values)
  }

  override def toSQL: String = List("class", " ", className.toSQL, fields.map(_.toSQL).mkString("(", ", ", ")")).mkString

}

object DeclareClass extends InvokableParser {
  private val templateCard = "class %a:name ( ?%P:fields )"

  override def parseInvokable(ts: TokenStream)(implicit compiler: SQLCompiler): DeclareClass = {
    val params = SQLTemplateParams(ts, templateCard)
    DeclareClass(className = params.atoms("name"), fields = params.parameters("fields"))
  }

  override def help: List[HelpDoc] = List(HelpDoc(
    name = "class",
    category = CATEGORY_SESSION,
    paradigm = PARADIGM_OBJECT_ORIENTED,
    description = "Creates a new ephemeral (in-memory) JVM-compatible class",
    syntax = templateCard,
    example =
      """|class Stocks(symbol: String, exchange: String, lastSale: Double, lastSaleTime: Date)
         |""".stripMargin
  ))

  override def understands(ts: TokenStream)(implicit compiler: SQLCompiler): Boolean = ts is "class"

}


