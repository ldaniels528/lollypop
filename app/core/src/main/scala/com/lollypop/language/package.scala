package com.lollypop

import com.lollypop.language.models.SourceCodeInstruction._
import com.lollypop.language.models._
import com.lollypop.runtime.instructions.conditions._
import com.lollypop.runtime.instructions.expressions._
import com.lollypop.runtime.instructions.functions.FunctionArguments
import com.lollypop.runtime.instructions.operators.ComputeAndSet.ComputeAndSetSugar
import com.lollypop.runtime.instructions.operators._
import com.lollypop.runtime.instructions.queryables.AssumeQueryable
import com.lollypop.runtime.{DataObject, DatabaseObjectRef}
import com.lollypop.util.StringRenderHelper.StringRenderer
import lollypop.lang.Null

import java.net.URL
import scala.collection.mutable
import scala.language.{implicitConversions, postfixOps, reflectiveCalls}

/**
 * Language package object
 */
package object language {

  /////////////////////////////////////////////////////////////////////////////////////////////////
  //      MANAGED EXCEPTIONS
  /////////////////////////////////////////////////////////////////////////////////////////////////

  @inline
  def dieArgumentMismatch(args: Int, minArgs: Int, maxArgs: Int): Nothing = {
    die(message =
      if (minArgs == 0 && maxArgs == 0) "No arguments were expected"
      else if (minArgs == maxArgs) s"Exactly $minArgs argument(s) expected"
      else if (args < minArgs) s"At least $minArgs argument(s) expected"
      else "Too many arguments"
    )
  }

  /////////////////////////////////////////////////////////////////////////////////////////////////
  //      CONVENIENCE METHODS FOR EXCEPTIONS
  /////////////////////////////////////////////////////////////////////////////////////////////////

  @inline def dieExpectedArray(): Nothing = die("An array value was expected")

  @inline def dieExpectedJSONObject(v: Any): Nothing = die(s"A JSON object was expected '$v' (${v.getClass.getName})")

  @inline def diePointerExpected(value: Any): Nothing = die(s"Expected pointer but got '$value' (${Option(value).map(_.getClass.getSimpleName).orNull})")

  @inline def dieFileNotFound(ref: DatabaseObjectRef): Nothing = die(s"No file or directory could be determine for table '${ref.toSQL}'")

  @inline def dieFunctionNotCompilable(ref: DatabaseObjectRef): Nothing = die(s"Function '${ref.toSQL}' could not be compiled")

  @inline def dieIllegalObjectRef(path: String): Nothing = die(s"Illegal object reference '$path'")

  @inline def dieIllegalType(value: Any): Nothing = die(s"Unexpected type returned '$value' (${Option(value).map(_.getClass.getSimpleName).orNull})")

  @inline def dieNoResultSet(): Nothing = die("Result set expected")

  @inline def dieNoSuchColumn(name: String): Nothing = die(s"Column '$name' not found")

  @inline def dieNoSuchColumnOrVariable(name: String): Nothing = die(s"Column or variable '$name' not found")

  @inline def dieNoSuchConstructor(name: String): Nothing = die(s"No suitable constructor found for class '$name'")

  @inline def dieNoSuchFunction(name: String): Nothing = die(s"Function '$name' was not found")

  @inline def dieObjectAlreadyExists(ref: DatabaseObjectRef): Nothing = die(s"Object '${ref.toSQL}' already exists")

  @inline def dieObjectIsNotADatabaseDevice(ref: DatabaseObjectRef): Nothing = die(s"Object '$ref' is not a database device")

  @inline def dieObjectIsNotAUserFunction(ref: DatabaseObjectRef): Nothing = die(s"Object '${ref.toSQL}' is not a user function")

  @inline def dieTableIsReadOnly(): Nothing = die("Table is read-only")

  @inline def dieUnsupportedConversion(v: Any, typeName: String): Nothing = {
    die(s"Conversion from '${v.renderAsJson.limit(30)}' (${v.getClass.getName}) to $typeName is not supported")
  }

  @inline def dieUnsupportedEntity(i: Instruction, entityName: String): Nothing = die(s"Unsupported $entityName: ${i.toSQL} (${i.getClass.getName})")

  @inline def dieUnsupportedType(v: Any): Nothing = die(s"Unsupported type $v (${v.getClass.getName})")

  /////////////////////////////////////////////////////////////////////////////////////////////////
  //      STRING-RELATED
  /////////////////////////////////////////////////////////////////////////////////////////////////

  private type StringLike = {
    def indexOf(s: String): Int
    def indexOf(s: String, start: Int): Int
    def lastIndexOf(s: String): Int
    def lastIndexOf(s: String, start: Int): Int
  }

  /////////////////////////////////////////////////////////////////////////////////////////////////
  //      IMPLICITS
  /////////////////////////////////////////////////////////////////////////////////////////////////

  final implicit class AsFunctionArguments(val expression: Expression) extends AnyVal {
    @inline
    def asArguments: List[Expression] = expression match {
      case FunctionArguments(args) => args
      case z => z.dieIllegalType()
    }
  }

  final implicit class EnrichedAssumeCondition(val instruction: Instruction) extends AnyVal {
    @inline
    def asCondition: Condition = instruction match {
      case condition: Condition => condition
      case other => AssumeCondition(other)
    }
  }

  final implicit class EnrichedAssumeExpression(val instruction: Instruction) extends AnyVal {
    @inline
    def asExpression: Expression = instruction match {
      case expression: Expression => expression
      case other => AssumeExpression(other)
    }
  }

  final implicit class EnrichedAssumeQueryable(val instruction: Instruction) extends AnyVal {
    @inline
    def asQueryable: Queryable = instruction match {
      case queryable: Queryable => queryable
      case other => AssumeQueryable(other)
    }
  }

  /**
   * Condition Extensions
   * @param expr0 the given [[Expression value]]
   */
  final implicit class ConditionExtensions(val expr0: Expression) extends AnyVal {

    @inline def between(from: Expression, to: Expression): Condition = Between(expr0, from, to)

    @inline def betwixt(from: Expression, to: Expression): Condition = Betwixt(expr0, from, to)

    @inline def in(query: Instruction): Condition = IN(expr0, query)

    @inline def in(values: Expression*): Condition = IN(expr0, ArrayLiteral(values: _*))

    @inline def isNotNull: Condition = expr0.isnt(Null())

    @inline def isNull: Condition = expr0.is(Null())

  }

  /**
   * Inequality Extensions
   * @param expr0 the given [[Expression value]]
   */
  final implicit class InequalityExtensions(val expr0: Expression) extends AnyVal {

    @inline def is(expr1: Expression): Is = Is(expr0, expr1)

    @inline def isnt(expr1: Expression): Isnt = Isnt(expr0, expr1)

    @inline def ===(expr1: Expression): EQ = EQ(expr0, expr1)

    @inline def !==(expr1: Expression): NEQ = NEQ(expr0, expr1)

    @inline def >(expr1: Expression): GT = GT(expr0, expr1)

    @inline def >=(expr1: Expression): GTE = GTE(expr0, expr1)

    @inline def <(expr1: Expression): LT = LT(expr0, expr1)

    @inline def <=(expr1: Expression): LTE = LTE(expr0, expr1)

  }

  /**
   * Item Sequence Utilities
   * @param items the collection of items
   * @tparam A the item type
   */
  final implicit class ItemSeqUtilities[A](val items: Seq[A]) extends AnyVal {
    @inline
    def onlyOne(ts: TokenStream): A = items.toList match {
      case value :: Nil => value
      case _ => ts.dieMultipleColumnsNotSupported()
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

  final implicit class MappedParameters[T](val mapped: Map[String, T]) extends AnyVal {
    def is(name: String, f: T => Boolean): Boolean = mapped.get(name).exists(f)
  }

  /**
   * Operation Extensions
   * @param expr0 the given [[Expression value]]
   */
  final implicit class RichOperation(val expr0: Expression) extends AnyVal {

    @inline def +(expr1: Expression): Plus = Plus(expr0, expr1)

    @inline def +=(expr1: Expression): ComputeAndSet = Plus(expr0, expr1).doAndSet

    @inline def ++(expr1: Expression): PlusPlus = PlusPlus(expr0, expr1)

    @inline def &(expr1: Expression): Amp = Amp(expr0, expr1)

    @inline def &=(expr1: Expression): ComputeAndSet = Amp(expr0, expr1).doAndSet

    @inline def &&(expr1: Expression): AmpAmp = AmpAmp(expr0, expr1)

    @inline def &&=(expr1: Expression): ComputeAndSet = AmpAmp(expr0, expr1).doAndSet

    @inline def |(expr1: Expression): Bar = Bar(expr0, expr1)

    @inline def |=(expr1: Expression): ComputeAndSet = Bar(expr0, expr1).doAndSet

    @inline def ||(expr1: Expression): BarBar = BarBar(expr0, expr1)

    @inline def ||=(expr1: Expression): ComputeAndSet = BarBar(expr0, expr1).doAndSet

    @inline def ::(expr1: Expression): ColonColon = ColonColon(expr1, expr0)

    @inline def ::=(expr1: Expression): ComputeAndSet = ColonColon(expr0, expr1).doAndSet

    @inline def :::(expr1: Expression): ColonColonColon = ColonColonColon(expr1, expr0)

    @inline def :::=(expr1: Expression): ComputeAndSet = ColonColonColon(expr0, expr1).doAndSet

    @inline def ^(expr1: Expression): Up = Up(expr0, expr1)

    @inline def ^=(expr1: Expression): ComputeAndSet = Up(expr0, expr1).doAndSet

    @inline def /(expr1: Expression): Div = Div(expr0, expr1)

    @inline def /=(expr1: Expression): ComputeAndSet = Div(expr0, expr1).doAndSet

    @inline def >>(expr1: Expression): GreaterGreater = GreaterGreater(expr0, expr1)

    @inline def >>=(expr1: Expression): ComputeAndSet = GreaterGreater(expr0, expr1).doAndSet

    @inline def >>>(expr1: Expression): GreaterGreaterGreater = GreaterGreaterGreater(expr0, expr1)

    @inline def >>>=(expr1: Expression): ComputeAndSet = GreaterGreaterGreater(expr0, expr1).doAndSet

    @inline def <<(expr1: Expression): LessLess = LessLess(expr0, expr1)

    @inline def <<=(expr1: Expression): ComputeAndSet = LessLess(expr0, expr1).doAndSet

    @inline def <<<(expr1: Expression): LessLessLess = LessLessLess(expr0, expr1)

    @inline def <<<=(expr1: Expression): ComputeAndSet = LessLessLess(expr0, expr1).doAndSet

    @inline def %(expr1: Expression): Percent = Percent(expr0, expr1)

    @inline def %%(expr1: Expression): PercentPercent = PercentPercent(expr0, expr1)

    @inline def %=(expr1: Expression): ComputeAndSet = Percent(expr0, expr1).doAndSet

    @inline def *(expr1: Expression): Times = Times(expr0, expr1)

    @inline def *=(expr1: Expression): ComputeAndSet = Times(expr0, expr1).doAndSet

    @inline def **(expr1: Expression): TimesTimes = TimesTimes(expr0, expr1)

    @inline def **=(expr1: Expression): ComputeAndSet = TimesTimes(expr0, expr1).doAndSet

    @inline def -(expr1: Expression): Minus = Minus(expr0, expr1)

    @inline def --(expr1: Expression): MinusMinus = MinusMinus(expr0, expr1)

    @inline def -=(expr1: Expression): ComputeAndSet = Minus(expr0, expr1).doAndSet

  }

  /**
   * Option Enrichment
   * @param optionA the given [[Option option]]
   */
  final implicit class OptionEnrichment[A](val optionA: Option[A]) extends AnyVal {

    @inline def ??[B <: A](optionB: => Option[B]): Option[A] = if (optionA.nonEmpty) optionA else optionB

    @inline def ||(value: => A): A = optionA getOrElse value

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

  /**
   * StringLike Index Enrichment
   * @param string the given [[StringLike]]; e.g. a [[String]] or [[StringBuilder]]
   */
  final implicit class StringLikeIndexEnrichment[T <: StringLike](val string: T) extends AnyVal {

    @inline def indexOfOpt(s: String): Option[Int] = toOption(string.indexOf(s))

    @inline def indexOfOpt(s: String, start: Int): Option[Int] = toOption(string.indexOf(s, start))

    @inline def lastIndexOfOpt(s: String): Option[Int] = toOption(string.lastIndexOf(s))

    @inline def lastIndexOfOpt(s: String, limit: Int): Option[Int] = toOption(string.lastIndexOf(s, limit))

    @inline
    private def toOption(index: Int): Option[Int] = index match {
      case -1 => None
      case _ => Some(index)
    }

  }

  /**
   * String Enrichment
   * @param string the given [[String]]
   */
  final implicit class StringEnrichment(val string: String) extends AnyVal {

    @inline
    def delimitedSplit(delimiter: Char, withQuotes: Boolean = false): List[String] = {
      var inQuotes = false
      var leftOver = false
      val sb = new mutable.StringBuilder()
      val values = string.toCharArray.foldLeft[List[String]](Nil) {
        case (list, ch) if ch == '"' =>
          inQuotes = !inQuotes
          if (withQuotes) sb.append(ch)
          list
        case (list, ch) if inQuotes & ch == delimiter =>
          sb.append(ch); list
        case (list, ch) if !inQuotes & ch == delimiter =>
          val s = sb.toString()
          sb.clear()
          leftOver = true
          list ::: s.trim :: Nil
        case (list, ch) =>
          leftOver = false
          sb.append(ch)
          list
      }
      if (sb.toString().nonEmpty || leftOver) values ::: sb.toString().trim :: Nil else values
    }

    @inline def toResourceURL: URL = getClass().getResource(string)

    @inline
    def isQuoted: Boolean = {
      (for {
        a <- string.headOption
        b <- string.lastOption
      } yield a == b && (a == '"' || a == '\'') || a == '`') contains true
    }

    @inline
    def limit(length: Int, ellipses: String = " ... "): String = {
      if (string.length < length) string else string.take(length) + ellipses
    }

    @inline def noneIfBlank: Option[String] = string.trim match {
      case s if s.isEmpty => None
      case s => Option(s)
    }

    @inline def singleLine: String = string.split("\n").map(_.trim).mkString(" ")

    @inline def toCamelCase: String = string match {
      case s if s.length > 1 => s.head.toLower + s.tail
      case s => s.toLowerCase
    }
  }

  /**
   * Tag Instruction With Line Numbers
   * @param op the [[Instruction instruction]]
   * @tparam A the instruction type
   */
  final implicit class TagInstructionWithLineNumbers[A <: Instruction](val op: A) extends AnyVal {
    /**
     * tag an instruction with line number and column information
     * @param _token the [[Token token]] containing the line number and column information
     * @return the tagged [[Instruction instruction]]
     */
    @inline
    def tag(_token: Option[Token]): A = {
      op.updateLocation(_token)
      op
    }
  }

  final implicit class RichToken[A <: Token](val token: A) extends AnyVal {
    @inline
    def withLineAndColumn(lineNo: Int, columnNo: Int): A = {
      token.lineNo = lineNo
      token.columnNo = columnNo
      token
    }
  }

  /**
   * Token Extensions
   * @param token the given [[Token]]
   */
  final implicit class TokenExtensions[A <: Token](val token: A) extends AnyVal {

    @inline def isIdentifier: Boolean = !TemplateProcessor.keywords.exists(token is) && {
      def isValidChar(c: Char): Boolean = c == '_' || c.isLetterOrDigit || (c >= 128)

      val value = token.valueAsString
      val legal_0 = (c: Char) => c == '$' || isValidChar(c)
      val legal_1 = (c: Char) => c == '.' || (c >= '0' && c <= '9') || isValidChar(c)
      (value.length == 1 && value.headOption.exists(c => c >= 0x80.toChar && c <= 0xff.toChar)) ||
        (value.headOption.exists(legal_0) && value.tail.forall(legal_1))
    }
  }

  /**
   * TokenStream Extensions
   * @param ts the given [[TokenStream]]
   */
  final implicit class TokenStreamExtensions(val ts: TokenStream) extends AnyVal {

    @inline def isField: Boolean = (ts.isBackticks || ts.isIdentifier) && !ts.isFunctionCall

    @inline def isFunctionCall: Boolean = (for {
      name <- ts.peekAhead(0) if name.isIdentifier
      paren0 <- ts.peekAhead(1) if (paren0 is "(") & (name.lineNo == paren0.lineNo)
      _ <- ts.peekAhead(2) // paren1 or arg0
    } yield true).contains(true)

    @inline def isIdentifier: Boolean = ts.peek.exists(_.isIdentifier) && !ts.isKeyword

    @inline def isKeyword: Boolean = {
      !ts.isQuoted && !ts.isBackticks && ts.peek.exists(t => TemplateProcessor.keywords.contains(t.valueAsString))
    }

    @inline
    def isPreviousTokenOnSameLine: Boolean = {
      (for {
        ts0 <- ts.lookBehind(1)
        t0 <- ts0.peek
        t1 <- ts.peek
      } yield t0.lineNo == t1.lineNo).contains(true)
    }

    @inline def nextIdentifier: Option[String] = if (isIdentifier) Some(ts.next().valueAsString) else None

    @inline
    def nextTableDotNotation(): DatabaseObjectRef = {

      def getNameComponent(ts: TokenStream): String = {
        if (ts.isBackticks || ts.isQuoted || ts.isText) ts.next().valueAsString else ts.dieExpectedTableNotation()
      }

      // gather the table components
      var list: List[String] = List(getNameComponent(ts))
      while (ts.peek.exists(_.valueAsString == ".")) {
        ts.next()
        list = getNameComponent(ts) :: list
      }

      // is there a column reference?
      val path = list.reverse.mkString(".") + (if (ts.nextIf("#")) "#" + getNameComponent(ts) else "")

      // return the table reference
      DatabaseObjectRef(path)
    }

  }

  final implicit class RichPeekableIterator(val iter: PeekableIterator[String]) extends AnyVal {
    def whilst[A](cond: PeekableIterator[String] => Boolean)(f: PeekableIterator[String] => A): List[A] = {
      var tags: List[A] = Nil
      while (cond(iter)) tags = f(iter) :: tags
      tags.reverse
    }
  }

  /**
   * Token Stream Exceptions
   * @param ts the given [[TokenStream]]
   */
  final implicit class TokenStreamExceptions(val ts: TokenStream) extends AnyVal {

    @inline def die(message: => String, cause: Throwable = null): Nothing = throw SyntaxException(message, ts, cause)

    @inline
    def dieArgumentMismatch(args: Int, minArgs: Int, maxArgs: Int): Nothing = {
      die(message =
        if (minArgs == 0 && maxArgs == 0) "No arguments were expected"
        else if (minArgs == maxArgs) s"Exactly $minArgs argument(s) expected"
        else if (args < minArgs) s"At least $minArgs argument(s) expected"
        else "Too many arguments"
      )
    }

    @inline def dieExpectedAlteration(): Nothing = die("At least one alteration was expected")

    @inline def dieExpectedArray(): Nothing = die("Array value expected")

    @inline def dieExpectedBoolean(): Nothing = die("Boolean expression expected")

    @inline def dieExpectedClassType(): Nothing = die("Class type identifier expected")

    @inline def dieExpectedCondition(): Nothing = die("A condition was expected")

    @inline def dieExpectedConditionOrExpression(): Nothing = die("Expression or condition expected")

    @inline def dieExpectedEntityRef(): Nothing = die("A database object reference was expected")

    @inline def dieExpectedEnumIdentifier(): Nothing = die("Enum identifier expected")

    @inline def dieExpectedExpression(): Nothing = die("An expression was expected")

    @inline def dieExpectedField(): Nothing = die("Field identifier expected")

    @inline def dieExpectedIdentifier(): Nothing = die("Identifier expected")

    @inline def dieExpectedIfExists(): Nothing = die("if exists expected")

    @inline def dieExpectedIfNotExists(): Nothing = die("if not exists expected")

    @inline def dieExpectedParameter(): Nothing = die("An parameter was expected")

    @inline def dieExpectedKeywords(keywords: String*): Nothing = {
      val keywordString = keywords match {
        case Seq(oneWord) => oneWord
        case words =>
          val (head, tail) = (words.init, words.last)
          head.mkString(", ") + " or " + tail
      }
      die(s"Expected keyword $keywordString")
    }

    @inline def dieExpectedNumeric(): Nothing = die("Numeric value expected")

    @inline def dieExpectedObject(): Nothing = die(s"Object type (e.g. table) expected")

    @inline def dieExpectedOneArgument(): Nothing = die("A single class argument was expected")

    @inline def dieExpectedOrderedColumn(): Nothing = die("Order column definition expected")

    @inline def dieExpectedQueryable(): Nothing = die("A queryable was expected")

    @inline def dieExpectedInvokable(): Nothing = die("A statement was expected")

    @inline def dieExpectedScopeMutation(i: Instruction): Nothing = i.die("A scope mutation instruction was expected")

    @inline def dieExpectedTableNotation(): Nothing =
      die("""Table notation expected (e.g. "public.stocks" or "portfolio.public.stocks" or `Months of the Year`)""")

    @inline def dieIllegalVariableName(): Nothing = die("Invalid variable name")

    @inline def dieIllegalType(value: Any): Nothing = die(s"Unexpected type returned '$value' (${Option(value).map(_.getClass.getSimpleName).orNull})")

    @inline def dieMultipleColumnsNotSupported(): Nothing = die("Multiple columns are not supported")

    @inline def diePatternNotMatched(): Nothing = die("Pattern did not match")

    @inline def dieScalarVariableIncompatibleWithRowSets(): Nothing = die("Scalar variable references are not compatible with row sets")

    @inline def dieTemplateError(values: Seq[String]): Nothing = die(s"Unexpected template error: ${values.mkString(", ")}")

    @inline def dieUnrecognizedCommand(): Nothing = die("Unrecognized command")

  }

}
