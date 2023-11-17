package com.lollypop.language.models

import com.lollypop.language.TemplateProcessor.TagInstructionWithLineNumbers
import com.lollypop.language.{SQLCompiler, TokenStream}
import com.lollypop.runtime.DataObject
import com.lollypop.runtime.instructions.conditions.BooleanLiteral
import com.lollypop.runtime.instructions.expressions.{Dictionary, NamedFunctionCall}
import com.lollypop.runtime.instructions.functions.FunctionArguments
import com.lollypop.util.OptionHelper.OptionEnrichment

import scala.annotation.tailrec
import scala.language.implicitConversions

/**
 * Represents an expression; which in its simplest form is a value (boolean, number, string, etc.)
 * @author lawrence.daniels@gmail.com
 */
trait Expression extends Instruction {
  var alias: Option[String] = None

  def as(name: String): this.type = as(name = Option(name))

  def as(name: Option[String]): this.type = {
    this.alias = name
    this
  }

  def isAggregation: Boolean = false

  def isPrimitive: Boolean = false

  override def toSQL: String = alias.map(name => s"${super.wrapSQL} as $name").getOrElse(super.toSQL)

}

/**
 * Expression Companion
 */
object Expression {

  /**
   * Parses the next expression modifier (e.g. "where lastSale <= 1")
   * @param stream     the given [[TokenStream token stream]]
   * @param expression the source [[Expression expression]]
   * @return a new queryable having the specified modification
   * @example @stocks where lastSale <= 1 order by symbol limit 5
   */
  @tailrec
  def apply(stream: TokenStream, expression: Expression)(implicit compiler: SQLCompiler): Expression = {
    val t0 = stream.peek
    val newExpression: Expression = {
      compiler.ctx.getExpressionChain(stream, expression) getOrElse {
        expression match {
          case queryable: Queryable => Queryable(stream, queryable)
          case expression: Expression => compiler.nextExpression(stream, expr0 = Option(expression)) getOrElse expression
          case other => other
        }
      }.tag(t0)
    }

    // if we got back the same instruction, drop out.
    // otherwise, try to consume another
    if (expression == newExpression) compiler.nextExpression(stream, expr0 = Option(expression)) getOrElse expression
    else Expression(stream, newExpression)
  }

  /**
   * importable implicits
   */
  object implicits {

    /**
     * Boolean to Expression conversion
     * @param value the given Boolean value
     * @return the equivalent [[BooleanLiteral]]
     */
    final implicit def boolean2Expr(value: Boolean): BooleanLiteral = BooleanLiteral(value)

    /**
     * Byte to Expression conversion
     * @param value the given Byte value
     * @return the equivalent [[Expression]]
     */
    final implicit def byte2Expr(value: Byte): Expression = Literal(value)

    /**
     * Char to Expression conversion
     * @param value the given Char value
     * @return the equivalent [[Expression]]
     */
    final implicit def char2Expr(value: Char): Expression = Literal(value)

    /**
     * Date to Expression conversion
     * @param value the given Date value
     * @return the equivalent [[Expression]]
     */
    final implicit def date2Expr(value: java.util.Date): Expression = Literal(value)

    /**
     * Double to Expression conversion
     * @param value the given Double value
     * @return the equivalent [[Expression]]
     */
    final implicit def double2Expr(value: Double): Expression = Literal(value)

    /**
     * Float to Expression conversion
     * @param value the given Float value
     * @return the equivalent [[Expression]]
     */
    final implicit def float2Expr(value: Float): Expression = Literal(value)

    /**
     * Integer to Expression conversion
     * @param value the given Integer value
     * @return the equivalent [[Expression]]
     */
    final implicit def int2Expr(value: Int): Expression = Literal(value)

    /**
     * Long to Expression conversion
     * @param value the given Long value
     * @return the equivalent [[Expression]]
     */
    final implicit def long2Expr(value: Long): Expression = Literal(value)

    /**
     * Map to Expression conversion
     * @param values the given map of values
     * @return the equivalent [[Dictionary]]
     */
    final implicit def map2Expr(values: Map[String, Expression]): Dictionary = Dictionary(values.toSeq: _*)

    /**
     * Short Integer to Expression conversion
     * @param value the given Short integer value
     * @return the equivalent [[Expression]]
     */
    final implicit def short2Expr(value: Short): Expression = Literal(value)

    /**
     * String to Expression conversion
     * @param value the given String value
     * @return the equivalent [[Expression]]
     */
    final implicit def string2Expr(value: String): Expression = Literal(value)

    /**
     * Symbol to Field conversion
     * @param symbol the given field name
     * @return the equivalent [[FieldRef]]
     */
    final implicit def symbolToField(symbol: Symbol): FieldRef = FieldRef(symbol.name)

    /**
     * Symbol to Ordered Column conversion
     * @param symbol the given column name
     * @return the equivalent [[OrderColumn]]
     */
    final implicit def symbolToOrderColumn(symbol: Symbol): OrderColumn = OrderColumn(symbol.name, isAscending = true)

    final implicit class AsFunctionArguments(val expression: Expression) extends AnyVal {
      @inline def asArguments: List[Expression] = expression match {
        case FunctionArguments(args) => args
        case z => z.dieIllegalType()
      }
    }

    /**
     * Lifestyle Expression Conversions
     * @param value the host [[Any value]]
     */
    final implicit class LifestyleExpressionsAny(val value: Any) extends AnyVal {
      @inline def v: Expression = Literal(value)
    }

    /**
     * Lifestyle Expression Conversions
     * @param string the host [[String string]]
     */
    final implicit class LifestyleExpressions(val string: String) extends AnyVal {

      @inline def a: Atom = Atom(string.trim)

      @inline def c: Column = Column(string.trim)

      @inline def p: Parameter = Column(string.trim).toParameter

      @inline def po: Parameter = Column(string.trim).toParameter.copy(isOutput = true)

      @inline def ct: ColumnType = ColumnType(string.trim)

      @inline def ct(size: Int): ColumnType = ColumnType(string.trim, size)

      @inline def f: FieldRef = string.trim match {
        case "*" => AllFields
        case name => FieldRef(name)
      }

      @inline def fx(args: Expression*): NamedFunctionCall = NamedFunctionCall(string, args.toList)

      @inline def asc: OrderColumn = OrderColumn(string, isAscending = true)

      @inline def desc: OrderColumn = OrderColumn(string, isAscending = false)

    }

    /**
     * Rich Aliases
     * @param instruction the host [[Instruction instruction]]
     */
    final implicit class RichAliasable[T <: Instruction](val instruction: T) extends AnyVal {

      @inline
      def as(alias: String): T = instruction match {
        case expression: Expression => expression.as(name = alias).asInstanceOf[T]
        case other => other.dieObjectNoSupportForAliases()
      }

      @inline
      def getName: Option[String] = instruction match {
        case expression: Expression if expression.alias.nonEmpty => expression.alias
        case n: NamedExpression => Some(n.name)
        case r: DataObject => Some(r.ns.name)
        case _ => None
      }

      @inline
      def getNameOrDie: String = getName || instruction.die("Expressions must have an alias")

    }

  }

}
