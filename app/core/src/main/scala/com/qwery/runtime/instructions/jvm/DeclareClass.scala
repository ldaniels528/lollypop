package com.qwery.runtime.instructions.jvm

import com.qwery.language.HelpDoc.{CATEGORY_SESSION, PARADIGM_OBJECT_ORIENTED}
import com.qwery.language.models.Expression.implicits.AsFunctionArguments
import com.qwery.language.models.{Atom, Expression, ParameterLike}
import com.qwery.language.{HelpDoc, InvokableParser, SQLCompiler, SQLTemplateParams, TokenStream}
import com.qwery.runtime.instructions.invocables.RuntimeInvokable
import com.qwery.runtime.{Plastic, QweryVM, Scope}
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
case class DeclareClass(classRef: Atom, fields: List[ParameterLike]) extends RuntimeInvokable {

  override def invoke()(implicit scope: Scope): (Scope, IOCost, DeclareClass) = {
    val _class = this
    (scope.withVariable(classRef.name, value = _class, isReadOnly = true), IOCost.empty, _class)
  }

  def newInstance(args: Expression)(implicit scope: Scope): Plastic = {
    // create the construct arguments
    val _args = args.asArguments
    val nameValues = fields.zipWithIndex map { case (field, n) =>
      if (n < _args.size) field.name -> _args(n) else field.name -> (field.defaultValue || Null())
    }
    // return the new scope and constructed object
    val (names, exprs) = (nameValues.map(_._1), nameValues.map(_._2))
    val values = QweryVM.transform(scope, exprs)._3
    Plastic.newInstance(classRef.name, names, values)(scope.getUniverse.classLoader)
  }

  override def toSQL: String = List("class", " ", classRef.toSQL, fields.map(_.toSQL).mkString("(", ", ", ")")).mkString

}

object DeclareClass extends InvokableParser {
  private val templateCard = "class %a:name ( ?%P:fields )"

  override def parseInvokable(ts: TokenStream)(implicit compiler: SQLCompiler): DeclareClass = {
    val params = SQLTemplateParams(ts, templateCard)
    DeclareClass(classRef = params.atoms("name"), fields = params.parameters("fields"))
  }

  override def help: List[HelpDoc] = List(HelpDoc(
    name = "class",
    category = CATEGORY_SESSION,
    paradigm = PARADIGM_OBJECT_ORIENTED,
    description = "Creates a new ephemeral (in-memory) JVM-compatible class",
    syntax = templateCard,
    example =
      """|import "java.util.Date"
         |class Stocks(symbol: String, exchange: String, lastSale: Double, lastSaleTime: Date)
         |""".stripMargin
  ))

  override def understands(ts: TokenStream)(implicit compiler: SQLCompiler): Boolean = ts is "class"

}


