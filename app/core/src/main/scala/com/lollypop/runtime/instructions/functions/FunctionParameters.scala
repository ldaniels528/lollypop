package com.lollypop.runtime.instructions.functions

import com.lollypop.language.models.Expression.implicits.{LifestyleExpressions, RichAliasable}
import com.lollypop.language.models._
import com.lollypop.runtime.instructions.expressions.NamedFunctionCall

/**
 * Utility that provides an <code>unapply</code> for returning function parameters
 */
object FunctionParameters {

  def unapply(instruction: Instruction): Option[List[ParameterLike]] = {

    def isForbidden(op: Instruction): Boolean = op match {
      case _: AllFields.type => true
      case _: Condition => true
      case _: Function => true
      case fc: FunctionCall => fc.alias.isEmpty
      case _: Literal => true
      case _: Queryable => true
      case x => !x.isInstanceOf[Expression]
    }

    // collect the arguments
    val args = instruction match {
      case ParameterBlock(parameters) => Some(parameters)
      case ArgumentBlock(args) if args.exists(isForbidden) => None
      case ab: ArgumentBlock => Some(ab.args)
      case nfc: NamedFunctionCall if nfc.alias.nonEmpty =>
        Some(List(Parameter(name = nfc.getNameOrDie, `type` = ColumnType(name = nfc.name, typeArgs = nfc.args.map(_.toSQL),
          arrayArgs = Nil, nestedColumns = Nil))))
      case ii if isForbidden(ii) => None
      case id: IdentifierRef => Some(List(id))
      case _ => None
    }

    // ensure each argument is parameter compatible
    args map (_ map {
      case columnType: ColumnType => Parameter(name = columnType.getNameOrDie, `type` = columnType)
      case param: ParameterLike => param
      case ident: IdentifierRef if ident.alias.nonEmpty => Parameter(ident.getNameOrDie, `type` = ident.name.ct)
      case expr => Parameter(expr.getNameOrDie, `type` = "Any".ct)
    })
  }

}
