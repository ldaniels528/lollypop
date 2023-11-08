package com.lollypop.runtime.instructions.invocables

import com.lollypop.language.ColumnTypeParser.nextColumnType
import com.lollypop.language.HelpDoc.{CATEGORY_SCOPE_SESSION, PARADIGM_IMPERATIVE}
import com.lollypop.language._
import com.lollypop.language.models.{Atom, ColumnType, Instruction}
import com.lollypop.runtime.LollypopVM.implicits.InstructionExtensions
import com.lollypop.runtime.Scope
import com.lollypop.runtime.datatypes.{AnyType, DataType}
import com.lollypop.runtime.instructions.queryables.{RowsOfValues, TableLiteral}
import com.lollypop.util.OptionHelper.OptionEnrichment
import lollypop.io.IOCost

/**
 * Defines a new variable in the current scope.
 * @example {{{
 *  var counter: Int = 5
 *  x += 1
 * }}}
 * @example {{{
 *  val greeting: String = 'Hello World'
 * }}}
 * @param ref          the [[Atom variable]] for which to set
 * @param `type`       the variable [[ColumnType type]]
 * @param initialValue the variable's initial value
 * @param isReadOnly   indicates whether the variable is read-only
 */
case class ValVar(ref: Atom, `type`: Option[ColumnType], initialValue: Option[Instruction], isReadOnly: Boolean = false)
  extends RuntimeInvokable {

  override def execute()(implicit scope: Scope): (Scope, IOCost, Any) = {
    val (scopeR, costR, resultR) = initialValue.map(_.execute(scope)) || (scope, IOCost.empty, None)
    val dataType = `type`.map(_type => DataType.load(_type)(scope)) || AnyType
    (scopeR.withVariable(ref.name, `type` = dataType, resultR, isReadOnly), costR, null)
  }

  override def toSQL: String = {
    (List(if (isReadOnly) "val" else "var", " ", ref.toSQL) ::: `type`.toList.flatMap(t => List(": ", t.toSQL)) ::: initialValue.toList.map {
      case tl: TableLiteral => s" =\n${tl.toSQL}"
      case r: RowsOfValues => s" from ${r.toSQL}"
      case v => s" = ${v.toSQL}"
    }).mkString
  }

}

object ValVar extends InvokableParser with InsertValues {
  private val template: String = "%C(kw|val|var) %a:ref ?: +?%T:type = %i:value"
  private val keywords = Seq("val", "var")

  override def help: List[HelpDoc] = List(HelpDoc(
    name = "val",
    category = CATEGORY_SCOPE_SESSION,
    paradigm = PARADIGM_IMPERATIVE,
    syntax = template,
    description = "Creates a read-only variable",
    example = "val greeting: String = 'Hello World'"
  ), HelpDoc(
    name = "var",
    category = CATEGORY_SCOPE_SESSION,
    paradigm = PARADIGM_IMPERATIVE,
    syntax = template,
    description = "Creates a variable",
    example = "var customer_id: Int = 5"
  ))

  override def parseInvokable(ts: TokenStream)(implicit compiler: SQLCompiler): Option[ValVar] = {
    if (understands(ts)) {
      val isReadOnly = ts.next().valueAsString == "val"
      val ref = compiler.nextAtom(ts) || ts.dieIllegalVariableName()
      val columnType = if (ts.nextIf(":")) Some(nextColumnType(ts)) else None
      ts.expect("=")
      val initialValue = compiler.nextExpression(ts) getOrElse compiler.nextOpCodeOrDie(ts)
      Some(ValVar(ref, `type` = columnType, initialValue = Some(initialValue), isReadOnly = isReadOnly))
    } else None
  }

  override def understands(ts: TokenStream)(implicit compiler: SQLCompiler): Boolean = keywords.exists(ts is _)

}