package com.lollypop.language

import com.lollypop.die
import com.lollypop.language.ColumnTypeParser.nextColumnType
import com.lollypop.language.TemplateProcessor.infrastructureTypes
import com.lollypop.language.TemplateProcessor.tags._
import com.lollypop.language.models._
import com.lollypop.runtime.DatabaseObjectRef
import com.lollypop.runtime.instructions.expressions.Dictionary
import com.lollypop.runtime.instructions.queryables.From
import org.slf4j.LoggerFactory

import scala.collection.concurrent.TrieMap
import scala.collection.mutable
import scala.language.postfixOps

/**
 * Represents a template processor
 * @author lawrence.daniels@gmail.com
 */
trait TemplateProcessor {

  /**
   * Parses an atom from the given stream
   * @param stream the given [[TokenStream stream]]
   * @return the option of a [[Atom]]
   */
  def nextAtom(stream: TokenStream): Option[Atom] = stream match {
    case ts if ts.isText | ts.isBackticks | ts.isQuoted => Some(Atom(ts.next().valueAsString))
    case _ => None
  }

  /**
   * Parses a header key (e.bg. "Accepts" or "Content-Type") from the given stream
   * @param stream the given [[TokenStream stream]]
   * @return the option of a [[String]]
   */
  def nextHeaderKey(stream: TokenStream): Option[String] = {
    if (stream.isText) {
      var list: List[String] = Nil
      while (stream.hasNext && stream.isText && !stream.isQuoted && !stream.isBackticks) {
        list = stream.next().valueAsString :: list
        if (stream.nextIf("-")) list = "-" :: list
      }
      Option(list.reverse.mkString)
    } else None
  }

  /**
   * Parses a conditional expression from the given stream
   * @param stream the given [[TokenStream stream]]
   * @return the option of a [[Condition]]
   */
  def nextCondition(stream: TokenStream): Option[Condition] = {
    nextExpression(stream) map {
      case condition: Condition => condition
      case expr => expr.asCondition
    }
  }

  def nextCodeBlockOrDictionary(stream: TokenStream): Option[Instruction] = {
    if (stream is "{") {
      val savePoint = stream.getPosition
      try nextCodeBlock(stream) catch {
        case _: Throwable =>
          stream.position = savePoint
          nextDictionary(stream)
      }
    } else None
  }

  def nextDictionaryOrCodeBlock(stream: TokenStream): Option[Expression] = {
    if (stream is "{") {
      val savePoint = stream.getPosition
      try nextDictionary(stream) catch {
        case _: Throwable =>
          stream.position = savePoint
          nextCodeBlock(stream).map(_.asExpression)
      }
    } else None
  }

  private def nextCodeBlock(ts: TokenStream): Option[CodeBlock] = {
    if (ts nextIf "{") {
      var statements: List[Instruction] = Nil
      while (ts isnt "}") statements = nextOpCodeOrDie(ts) :: statements
      ts.expect("}")
      Option(CodeBlock(statements.reverse))
    } else None
  }

  private def nextDictionary(stream: TokenStream): Option[Dictionary] = {
    stream match {
      case ts if ts nextIf "{" =>
        var entries: List[(String, Expression)] = Nil
        do {
          do {
            // get the next dictionary entry (including 'Accept-Encoding')
            nextExpression(ts, isDictionary = true) match {
              case Some(entry) => entries = entries ::: entry.getNameOrDie -> entry :: Nil
              case None => ts.dieExpectedExpression()
            }
          } while (ts nextIf ",")
        } while (ts isnt "}")
        ts.expect("}")
        Some(Dictionary(entries: _*))
      case _ => None
    }
  }

  /**
   * Parses an expression or condition from the given stream
   * @param stream the given [[TokenStream stream]]
   * @return the option of a [[Expression expression]]
   */
  def nextExpression(stream: TokenStream,
                     expr0: Option[Expression] = None,
                     isDictionary: Boolean = false,
                     preventAliases: Boolean = false): Option[Expression]

  /**
   * Creates a new field from a token stream
   * @param stream the given [[TokenStream token stream]]
   * @return a new [[FieldRef field reference]] instance
   */
  def nextField(stream: TokenStream): Option[FieldRef] = {
    stream match {
      case ts if ts nextIf "*" => Some(AllFields)
      case ts if ts.isField => Some(FieldRef(ts.next().valueAsString))
      case ts if ts.isNumeric => Some(FieldRef(ts.next().valueAsString))
      case _ => None
    }
  }

  /**
   * Creates a new field from a token stream
   * @param stream the given [[TokenStream token stream]]
   * @return a new [[Expression field reference]] instance
   */
  def nextFieldOrColumnType(stream: TokenStream): Option[Expression] = {

    def convertToType(instruction: Expression): Expression = {
      instruction match {
        case ii: IdentifierRef if ii.name != "*" =>
          // array type?
          if (stream nextIf "[ ]") ColumnType.array(columnType = ii.name.ct) else ii
        case ee => ee
      }
    }

    // interpret the field reference or column type
    stream match {
      case ts if ts nextIf "*" => Some(AllFields)
      case ts if ts.isField => Some(FieldRef(ts.next().valueAsString)).map(convertToType)
      case ts if ts.isNumeric => Some(FieldRef(ts.next().valueAsString)).map(convertToType)
      case _ => None
    }
  }

  def nextInfrastructureType(stream: TokenStream): String = {
    infrastructureTypes.find(stream nextIf _) match {
      case Some(_type) => _type
      case None => stream.dieExpectedObject()
    }
  }

  /**
   * Extracts a variable number of function arguments
   * @param stream the given [[TokenStream token stream]]
   * @return a collection of [[Expression argument expressions]]
   */
  def nextListOfArguments(stream: TokenStream): List[Expression] = {
    var expressions: List[Expression] = Nil
    stream.capture(begin = "(", end = ")", delimiter = Some(",")) { ts =>
      expressions = (nextExpression(ts) || ts.dieExpectedExpression()) :: expressions
    }
    expressions.reverse
  }

  def nextListOfExpressions(ts: TokenStream): List[Expression] = {
    var expressions: List[Expression] = Nil
    do expressions = (nextExpression(ts) || ts.dieExpectedExpression()) :: expressions while (ts nextIf ",")
    expressions.reverse
  }

  def nextListOfFields(ts: TokenStream): List[FieldRef] = {
    var fields: List[FieldRef] = Nil
    do fields = (nextField(ts) || ts.dieExpectedField()) :: fields while (ts nextIf ",")
    fields.reverse
  }

  def nextListOfFunctionParameters(stream: TokenStream)(implicit compiler: SQLCompiler): List[ParameterLike] = {
    var parameters: List[ParameterLike] = Nil
    stream.capture(begin = "(", end = ")", delimiter = Some(",")) { ts =>
      parameters = (nextParameter(ts) || ts.dieExpectedParameter()) :: parameters
    }
    parameters.reverse
  }

  private def nextParameter(stream: TokenStream)(implicit compiler: SQLCompiler): Option[ParameterLike] = {

    def updateWithOptions(column: Column): Column = {
      var columnA: Column = column
      var columnB: Column = column
      do {
        columnB = columnA
        columnA = stream match {
          case ts if ts nextIf "=" => columnA.copy(defaultValue = nextExpression(ts))
          case _ => columnA
        }
      } while (columnA != columnB)
      columnA
    }

    // parse the column or parameter
    val isOutput = stream nextIf "-->"
    for {
      Atom(name) <- nextAtom(stream)
      _type = stream match {
        case ts if ts.nextIf(":") => nextColumnType(ts)
        case ts if ts.peek.exists(_.isIdentifier) => nextColumnType(ts)
        case _ => "Any".ct
      }
      column = updateWithOptions(Column(name, _type))
    } yield if (isOutput) column.toParameter.copy(isOutput = isOutput) else column
  }

  def nextListOfParameters(stream: TokenStream)(implicit compiler: SQLCompiler): List[ParameterLike] = {
    var columns: List[ParameterLike] = Nil
    do {
      // get the basic options
      columns = (for {
        column <- nextParameter(stream).toList
      } yield column) ::: columns
    } while (stream nextIf ",")
    columns.reverse
  }

  /**
   * Parses the next query or statement from the stream
   * @param stream the given [[TokenStream token stream]]
   * @return the option of an [[Instruction]]
   */
  def nextOpCode(stream: TokenStream): Option[Instruction]

  /**
   * Parses the next query or statement from the stream
   * @param ts the given [[TokenStream token stream]]
   * @return an [[Instruction]]
   */
  def nextOpCodeOrDie(ts: TokenStream): Instruction = nextOpCode(ts) || ts.dieUnrecognizedCommand()

  /**
   * Optionally parses an alias (e.g. '( ... ) as O')
   * @param entity the given [[Instruction]]
   * @param ts     the given [[TokenStream]]
   * @return the resultant [[Instruction]]
   */
  private def nextOpCodeWithOptionalAlias(entity: Instruction, ts: TokenStream): Instruction = {
    if (ts nextIf "as") entity.as(alias = ts.next().valueAsString) else entity
  }

  def nextOrderedColumns(stream: TokenStream): List[OrderColumn] = {
    var sortFields: List[OrderColumn] = Nil
    do {
      // capture the column (e.g. "A.symbol" | "symbol")
      var column = stream match {
        case ts if ts.isText & ts(1).exists(_ is ".") =>
          val alias = ts.next().valueAsString
          val ascending = ts.next().valueAsString == "."
          val name = ts.next().valueAsString
          OrderColumn(name, ascending).as(alias)
        case ts if ts.isText | ts.isBackticks => OrderColumn(name = ts.next().valueAsString, isAscending = true)
        case ts => ts.dieExpectedOrderedColumn()
      }

      // determine whether the column is ascending or descending
      column = column.copy(isAscending = stream match {
        case ts if ts nextIf "asc" => true
        case ts if ts nextIf "desc" => false
        case _ => true
      })

      // append the column to our list
      sortFields = column :: sortFields
    } while (stream nextIf ",")
    sortFields.reverse
  }

  /**
   * Parses the next [[Queryable query]] or [[VariableRef variable]] with an optional alias from the stream
   * @param stream the given [[TokenStream token stream]]
   * @return the resultant [[Queryable]], [[DatabaseObjectRef]] or [[VariableRef]]
   */
  def nextQueryOrVariableWithAlias(stream: TokenStream): Instruction = {
    stream match {
      // indirect query?
      case ts if ts is "(" => nextOpCodeWithOptionalAlias(ts.extract(begin = "(", end = ")")(ts =>
        nextExpression(ts) match {
          case Some(queryable: Queryable) => queryable
          case Some(expression) => From(expression)
          case None => nextQueryOrVariableWithAlias(ts)
        }), ts)
      // scalar variable (e.g. "@results")?
      case ts if ts nextIf "$" => nextOpCodeWithOptionalAlias(nextTableVariable(ts), ts)
      // table variable (e.g. "@name")?
      case ts if ts nextIf "@" => nextOpCodeWithOptionalAlias(nextTableVariable(ts), ts)
      // sub-query?
      case ts => nextOpCodeOrDie(ts)
    }
  }

  /**
   * Parses the next [[Queryable query]], [[DatabaseObjectRef table]] or [[VariableRef variable]]
   * with an optional alias from the stream
   * @param stream the given [[TokenStream]]
   * @return the resultant [[Instruction]]
   */
  def nextQueryOrTableOrVariableWithAlias(stream: TokenStream): Instruction = {
    stream match {
      // table dot notation (e.g. "public"."stocks" or "public.stocks" or "`Months of the Year`")
      case ts if ts.isBackticks | ts.isDoubleQuoted | ts.isText => nextOpCodeWithOptionalAlias(ts.nextTableDotNotation(), ts)
      // must be a sub-query or variable
      case ts => nextQueryOrVariableWithAlias(ts)
    }
  }

  /**
   * Parses the next [[DatabaseObjectRef table]] or [[VariableRef variable]] from the stream
   * @param stream the given [[TokenStream]]
   * @return the resultant [[DatabaseObjectRef]]
   */
  def nextTableOrVariable(stream: TokenStream): DatabaseObjectRef = stream match {
    case ts if ts nextIf "$" => nextTableVariable(ts)
    case ts if ts nextIf "@" => nextTableVariable(ts)
    case ts if ts.isBackticks | ts.isText | ts.isQuoted => ts.nextTableDotNotation()
    case ts => ts.dieExpectedEntityRef()
  }

  /**
   * Parses a variable reference from the stream
   * @param stream the given [[TokenStream token stream]]
   * @return the option of a [[VariableRef]]
   */
  def nextVariableReference(stream: TokenStream): Option[VariableRef] = {
    val variable = stream match {
      case ts if ts nextIf "$" => ts.nextIdentifier.map(id => $(id))
      case ts if ts nextIf "@" => ts.nextIdentifier.map(id => @@(id))
      case ts => ts.nextIdentifier.map(id => $(id))
    }
    variable ?? stream.dieIllegalVariableName()
  }

  def nextTableVariable(ts: TokenStream): DatabaseObjectRef = {
    var tblVar: DatabaseObjectRef = @@(ts.nextIdentifier || ts.dieIllegalVariableName())
    if (ts.nextIf("#")) tblVar = DatabaseObjectRef.InnerTable(tblVar, ts.nextIdentifier || ts.dieIllegalVariableName())
    tblVar
  }

}

/**
 * Represents a template processor
 * @author lawrence.daniels@gmail.com
 */
object TemplateProcessor {
  private[this] val logger = LoggerFactory.getLogger(getClass)
  private val customTags = TrieMap[String, String => TemplateTag]()
  private val infrastructureTypes = List(
    "class",
    "external table",
    "function",
    "index",
    "macro",
    "procedure",
    "table",
    "type",
    "view"
  )
  val keywords: Set[String] = (infrastructureTypes.flatMap {
    case s if s contains " " => s.split(" ").toList
    case s => s :: Nil
  } ::: List(
    "do", "else", "false", "for", "from", "if", "insert", "limit", "not", "null",
    "recursive", "replace", "select", "then", "true", "update", "when", "where", "while"
  )).toSet

  /**
   * Registers a custom tag implementation
   * @param prefix the tag prefix // (e.g. "%PP:points" => "PP")
   * @param tag    a function that produces the requested [[TemplateTag tag implementation]]
   * @return self
   */
  def addTag(prefix: String, tag: String => TemplateTag): TemplateProcessor.type = {
    customTags(prefix) = tag
    this
  }

  def compile(templateString: String): List[TemplateTag] = {
    new PeekableIterator(templateString.cleanup.split(" ").map(_.trim)).whilst(_.hasNext)(parseNext)
  }

  def execute(stream: TokenStream, tags: List[TemplateTag])(implicit compiler: SQLCompiler): SQLTemplateParams = {
    tags.foldLeft(SQLTemplateParams()) { (params, tag) => params + tag.extract(stream) }
  }

  def executeWithDebug(stream: TokenStream, tags: List[TemplateTag])(implicit compiler: SQLCompiler): SQLTemplateParams = {
    tags.foldLeft(SQLTemplateParams()) { (params, tag) =>
      val result = params + tag.extract(stream)
      logger.info(s"|${tag.toCode}| => ${result.all} << $stream >>")
      result
    }
  }

  private def parseNext(iter: PeekableIterator[String]): TemplateTag = {
    parseNextTag(iter.next()) match {
      ////////////////////////////////////////////////
      // is it an optional tag ('?')?
      case parent: OptionalTemplateTag =>
        parent.copy(dependents = iter.whilst(_.peek.exists(_.startsWith("+?")))(parseNext).collect {
          case dependent: OptionalDependentTemplateTag => dependent
          case tag => die(s"Template syntax error near '$tag' (${tag.getClass.getName}) instead of a ${OptionalDependentTemplateTag.getClass.getName}")
        })
      ///////////////////////////////////////////////
      // is it an optional sequence tag ('O%')?
      case parent: OptionalSequenceTemplateTag if iter.peek.exists(_.endsWith("{{")) =>
        iter.next()
        val options = iter.whilst(!_.peek.exists(_.endsWith("}}")))(parseNext)
        iter.next()
        parent.copy(options = options)
      ///////////////////////////////////////////////
      // is it an optional multi-sequence tag ('OO%')?
      case parent: OptionalMultiSequenceTemplateTag if iter.peek.exists(_.endsWith("{{")) =>
        iter.next()
        val options = iter.whilst(!_.peek.exists(_.endsWith("}}")))(parseNext)
        iter.next()
        parent.copy(options = options)
      ///////////////////////////////////////////////
      // could anything other tag ...
      case tag => tag
    }
  }

  private def parseNextTag(tagString: String): TemplateTag = tagString match {
    // optionally required tag? (e.g. "?limit +?%e:limit" => "limit 100")
    case tag if tag.startsWith("?") => OptionalTemplateTag(tag = parseNextTag(tag drop 1))

    // optionally required child-tag? (e.g. "?ORDER +?by +?%o:sortFields" => "order by Symbol desc")
    case tag if tag.startsWith("+?") => OptionalDependentTemplateTag(tag = parseNextTag(tag drop 2))

    // atom? (e.g. "%a:name" => "Tickers" | "'Tickers'")
    case tag if tag.startsWith("%a:") => AtomTemplateTag(tag drop 3)

    // list of arguments? (e.g. "%A:args" => "(field1, 'hello', 5 + new `java.util.Date`(), ..., fieldN)")
    case tag if tag.startsWith("%A:") => ListOfArgumentsTemplateTag(tag drop 3)

    // conditional expression? (e.g. "%c:condition" => "x = 1 and y = 2")
    case tag if tag.startsWith("%c:") => ConditionTemplateTag(tag drop 3)

    // chooser? (e.g. "%C(mode,into,overwrite)" => "insert into ..." | "insert overwrite ...")
    case tag if tag.startsWith("%C(") & tag.endsWith(")") => ChooseTemplateTag(tag.chooserParams)

    // dictionary literal? (e.g. "%d:dictionary" => "{ "x": 1, "y": 2 }")
    case tag if tag.startsWith("%d:") => DictionaryLiteralTemplateTag(tag drop 3)

    // single expression? (e.g. "%e:expression" => "2 * (x + 1)")
    case tag if tag.startsWith("%e:") => ExpressionTemplateTag(tag drop 3)

    // list of expressions? (e.g. "%E:fields" => "field1, 'hello', 5 + new `java.util.Date`(), ..., fieldN")
    case tag if tag.startsWith("%E:") => ListOfExpressionsTemplateTag(tag drop 3)

    // single field? (e.g. "%f:field" => "accountNumber")
    case tag if tag.startsWith("%f:") => FieldTemplateTag(name = tag.drop(3))

    // list of fields? (e.g. "%F:fields" => "field1, field2, ..., fieldN")
    case tag if tag.startsWith("%F:") => ListOfFieldsTemplateTag(tag drop 3)

    // function parameters? (e.g. "%FP:params" => "(name String, age INTEGER, dob DATE)")
    case tag if tag.startsWith("%FP:") => ListOfFunctionParametersTemplateTag(tag drop 4)

    // expression on same line as last token? (e.g. "%g:expression" => "2 * (x + 1)")
    case tag if tag.startsWith("%g:") => ExpressionTemplateTag(tag drop 3, isPreviousTokenOnSameLine = true)

    // header key? (e.g. "%h:name" => "Accept-Encoding" | "Cookie")
    case tag if tag.startsWith("%h:") => HeaderKeyTemplateTag(tag drop 3)

    // instruction (expression, invocable or queryable)?
    case tag if tag.startsWith("%i:") => InstructionTemplateTag(name = tag.drop(3))

    // infrastructure type tag? (e.g. "function" or "view")
    case tag if tag.startsWith("%I:") => InfrastructureTypeTemplateTag(tag drop 3)

    // Java Class? (e.g. "%jc:class" => "java.util.Date")
    case tag if tag.startsWith("%jc:") => JavaClassTemplateTag(tag drop 4)

    // Java Jar? (e.g. "%jj:jar" => "libs/utils.jar")
    case tag if tag.startsWith("%jj:") => JavaJarTemplateTag(tag drop 4)

    // location or table? (e.g. "accounts" | "stocks#transactions" | @accounts)
    case tag if tag.startsWith("%L:") => TableOrVariableTemplateTag(tag drop 3)

    // multi-tag? (e.g. "%c,%q")
    case tag if tag.startsWith("%M:") =>
      tag.drop(3).split(':') match {
        case Array(codes, name) => MultiTagTemplateTag(name, codes.split(",").map(_.trim))
        case _ => die(s"Syntax error: [$tag] example: %M:%c,%q:name")
      }

    // next statement?
    case tag if tag.startsWith("%N:") => NextStatementTemplateTag(tag drop 3)

    // ordered field list? (e.g. "%o:orderedFields" => "field1 desc, field2 asc, field3")
    case tag if tag.startsWith("%o:") => OrderedColumnsTemplateTag(tag drop 3)

    // optional multi-sequence? (e.g. '%OO {{ ?user +?name +?is +?%e:username ?role +?is +?%e:role }}')
    case tag if tag.startsWith("%OO") => OptionalMultiSequenceTemplateTag()

    // optional sequence? (e.g. '%O {{ ?user +?name +?is +?%e:username ?role +?is +?%e:role }}')
    case tag if tag.startsWith("%O") => OptionalSequenceTemplateTag()

    // parameters? (e.g. "%P:params" => "name String, age INTEGER, dob DATE")
    case tag if tag.startsWith("%P:") => ListOfParametersTemplateTag(tag drop 3)

    // indirect query source (queries, tables and variables)? (e.g. "%q:source" => "AddressBook" | "( select * from AddressBook )" | "@addressBook")
    case tag if tag.startsWith("%q:") => QueryOrTableOrVariableTemplateTag(tag drop 3)

    // direct query source (queries and variables)? (e.g. "%Q:query" => "select * from AddressBook" | "@addressBook")
    case tag if tag.startsWith("%Q:") => QueryOrVariableTemplateTag(tag drop 3)

    // regular expression match? (e.g. "%r`\\d{3,4}\\S+`" => "123ABC")
    case tag if tag.startsWith("%r`") & tag.endsWith("`") => RegExTemplateTag(pattern = tag.drop(3).dropRight(1))

    // data type tag? (e.g. "Decimal(20,2)")
    case tag if tag.startsWith("%T:") => ColumnTypeTemplateTag(tag drop 3)

    // expression or condition? (e.g. "%x:value" => "4 > x")
    case tag if tag.startsWith("%x:") => ExpressionOrConditionTemplateTag(name = tag.drop(3))

    // escape tag? (e.g. "\%" => expect "%")
    case tag if tag == "\\?" => KeywordTemplateTag("%")
    case tag if tag == "\\%" => KeywordTemplateTag("%")

    // custom tag? (e.g. "%KVP:points")
    case tag if tag.startsWith("%") =>
      (for {
        start <- tag.indexOfOpt("%")
        end <- tag.indexOfOpt(":") if end > start
        prefix = tag.substring(start + 1, end) // (e.g. "%PP:points" => "PP")
        name = tag.substring(end + 1, tag.length) // (e.g. "%PP:points" => "points")
        impl <- customTags.get(prefix)
      } yield impl(name)) getOrElse die(s"Unrecognized template tag '$tag'")

    // must be a literal text (e.g. "from")
    case tag => KeywordTemplateTag(tag)
  }

  final implicit class RichStringCleanup(val s: String) extends AnyVal {

    /**
     * Removes non-printable characters and extraneous spaces
     * @return the cleaned string
     */
    @inline def cleanup: String = {
      val sb = new mutable.StringBuilder(s.map { case '\n' | '\r' | '\t' => ' '; case c => c }.trim)
      while (sb.indexOfOpt("  ").map(index => sb.replace(index, index + 2, " ")).nonEmpty) {}
      sb.toString()
    }
  }

  /**
   * Rich Template
   * @param tag the given tag string (e.g. "%C(mode,into,overwrite)")
   */
  final implicit class RichTemplate(val tag: String) extends AnyVal {

    /**
     * Extracts the chooser parameters (e.g. "%C(mode,into,overwrite)" => ["mode", "into", "overwrite"])
     */
    @inline def chooserParams: Array[String] = {
      val s = tag.drop(3).dropRight(1)
      s.indexWhere(c => !c.isLetterOrDigit && c != '.' && c != '_') match {
        case -1 => die("Chooser tags require a non-alphanumeric delimiter")
        case index =>
          val delimiter = s(index)
          s.split(delimiter).map(_.trim)
      }
    }
  }

  object tags {

    trait TemplateTag {

      def extract(stream: TokenStream)(implicit compiler: SQLCompiler): SQLTemplateParams

      def toCode: String

    }

    case class AtomTemplateTag(name: String) extends TemplateTag {
      override def extract(ts: TokenStream)(implicit compiler: SQLCompiler): SQLTemplateParams = {
        SQLTemplateParams(atoms = Map(name -> compiler.nextAtom(ts).getOrElse(ts.dieExpectedKeywords(name))))
      }

      override def toCode: String = s"%a:$name"
    }

    case class ChooseTemplateTag(values: Seq[String]) extends TemplateTag {
      override def extract(ts: TokenStream)(implicit compiler: SQLCompiler): SQLTemplateParams = {
        @inline def error[A](items: List[String]): A = ts.dieExpectedKeywords(items: _*)

        values.toList match {
          case name :: items =>
            val item = ts.peek.map(_.valueAsString).getOrElse(error(items))
            if (!items.contains(item)) error(items)
            ts.next() // must skip the token
            SQLTemplateParams(atoms = Map(name -> Atom(item)))
          case _ =>
            ts.dieTemplateError(values)
        }
      }

      override def toCode: String = s"%C(${values.mkString("|")})"
    }

    case class ColumnTypeTemplateTag(name: String) extends TemplateTag {
      override def extract(ts: TokenStream)(implicit compiler: SQLCompiler): SQLTemplateParams = {
        SQLTemplateParams(types = Map(name -> nextColumnType(ts)))
      }

      override def toCode: String = s"%T:$name"
    }

    case class ConditionTemplateTag(name: String) extends TemplateTag {
      override def extract(ts: TokenStream)(implicit compiler: SQLCompiler): SQLTemplateParams = {
        SQLTemplateParams(instructions = Map(name -> (compiler.nextCondition(ts) || ts.dieExpectedBoolean())))
      }

      override def toCode: String = s"%c:$name"
    }

    case class DictionaryLiteralTemplateTag(name: String) extends TemplateTag {
      override def extract(ts: TokenStream)(implicit compiler: SQLCompiler): SQLTemplateParams = {
        SQLTemplateParams(instructions = Map(name -> (compiler.nextDictionaryOrCodeBlock(ts) || ts.dieExpectedExpression())))
      }

      override def toCode: String = s"%d:$name"
    }

    case class InfrastructureTypeTemplateTag(name: String) extends TemplateTag {
      override def extract(ts: TokenStream)(implicit compiler: SQLCompiler): SQLTemplateParams = {
        SQLTemplateParams(atoms = Map(name -> Atom(compiler.nextInfrastructureType(ts))))
      }

      override def toCode: String = s"%I:$name"
    }

    case class ExpressionOrConditionTemplateTag(name: String) extends TemplateTag {
      override def extract(ts: TokenStream)(implicit compiler: SQLCompiler): SQLTemplateParams = {
        SQLTemplateParams(instructions = Map(name -> (compiler.nextExpression(ts) || ts.dieExpectedConditionOrExpression())))
      }

      override def toCode: String = s"%x:$name"
    }

    case class ExpressionTemplateTag(name: String, isPreviousTokenOnSameLine: Boolean = false)
      extends TemplateTag {
      override def extract(ts: TokenStream)(implicit compiler: SQLCompiler): SQLTemplateParams = {
        val nextExpr = if (!isPreviousTokenOnSameLine || ts.isPreviousTokenOnSameLine) compiler.nextExpression(ts) else None
        SQLTemplateParams(instructions = Map(name -> (nextExpr || ts.dieExpectedExpression())))
      }

      override def toCode: String = if (isPreviousTokenOnSameLine) s"%g:$name" else s"%e:$name"
    }

    case class FieldTemplateTag(name: String) extends TemplateTag {
      override def extract(ts: TokenStream)(implicit compiler: SQLCompiler): SQLTemplateParams = {
        SQLTemplateParams(instructions = Map(name -> (compiler.nextField(ts) || ts.dieExpectedField())))
      }

      override def toCode: String = s"%f:$name"
    }

    case class HeaderKeyTemplateTag(name: String) extends TemplateTag {
      override def extract(ts: TokenStream)(implicit compiler: SQLCompiler): SQLTemplateParams = {
        SQLTemplateParams(atoms = Map(name -> Atom(compiler.nextHeaderKey(ts).getOrElse(ts.dieExpectedKeywords(name)))))
      }

      override def toCode: String = s"%h:$name"
    }

    case class InstructionTemplateTag(name: String) extends TemplateTag {
      override def extract(ts: TokenStream)(implicit compiler: SQLCompiler): SQLTemplateParams = {
        compiler.nextOpCode(ts) match {
          case Some(instruction) => SQLTemplateParams(instructions = Map(name -> instruction))
          case None => SQLTemplateParams(instructions = Map(name -> (compiler.nextExpression(ts) || ts.dieExpectedExpression())))
        }
      }

      override def toCode: String = s"%i:$name"
    }

    case class JavaClassTemplateTag(name: String) extends TemplateTag {
      override def extract(ts: TokenStream)(implicit compiler: SQLCompiler): SQLTemplateParams = {
        SQLTemplateParams(ts, s"class ( %a:$name )")
      }

      override def toCode: String = s"%jc:$name"
    }

    case class JavaJarTemplateTag(name: String) extends TemplateTag {
      override def extract(ts: TokenStream)(implicit compiler: SQLCompiler): SQLTemplateParams = {
        SQLTemplateParams(ts, s"jar ( %a:$name )")
      }

      override def toCode: String = s"jj:$name"
    }

    case class KeywordTemplateTag(keyword: String) extends TemplateTag {
      override def extract(ts: TokenStream)(implicit compiler: SQLCompiler): SQLTemplateParams = {
        if (ts nextIf keyword) SQLTemplateParams(keywords = Set(keyword)) else ts.dieExpectedKeywords(keyword)
      }

      override def toCode: String = keyword
    }

    case class ListOfArgumentsTemplateTag(name: String) extends TemplateTag {
      override def extract(stream: TokenStream)(implicit compiler: SQLCompiler): SQLTemplateParams = {
        SQLTemplateParams(expressionLists = Map(name -> compiler.nextListOfArguments(stream)))
      }

      override def toCode: String = s"%A:$name"
    }

    case class ListOfExpressionsTemplateTag(name: String) extends TemplateTag {
      override def extract(stream: TokenStream)(implicit compiler: SQLCompiler): SQLTemplateParams = {
        SQLTemplateParams(expressionLists = Map(name -> compiler.nextListOfExpressions(stream)))
      }

      override def toCode: String = s"%E:$name"

    }

    case class ListOfFieldsTemplateTag(name: String) extends TemplateTag {
      override def extract(stream: TokenStream)(implicit compiler: SQLCompiler): SQLTemplateParams = {
        SQLTemplateParams(fieldLists = Map(name -> compiler.nextListOfFields(stream)))
      }

      override def toCode: String = s"%F:$name"
    }

    case class ListOfFunctionParametersTemplateTag(name: String) extends TemplateTag {
      override def extract(stream: TokenStream)(implicit compiler: SQLCompiler): SQLTemplateParams = {
        SQLTemplateParams(parameters = Map(name -> compiler.nextListOfFunctionParameters(stream)))
      }

      override def toCode: String = s"%FP:$name"
    }

    case class ListOfParametersTemplateTag(name: String) extends TemplateTag {
      override def extract(stream: TokenStream)(implicit compiler: SQLCompiler): SQLTemplateParams = {
        SQLTemplateParams(parameters = Map(name -> compiler.nextListOfParameters(stream)))
      }

      override def toCode: String = s"%P:$name"
    }

    case class MultiTagTemplateTag(name: String, codes: Seq[String]) extends TemplateTag {
      override def extract(stream: TokenStream)(implicit compiler: SQLCompiler): SQLTemplateParams = {
        codes.map(tag => s"$tag:$name").collectFirst { case tag if stream.matches(tag) =>
          val template = parseNextTag(tag)
          template.extract(stream)
        } getOrElse die(s"Could not find a match: $toCode")
      }

      override def toCode: String = s"%M:${codes.mkString(",")}:$name"
    }

    case class NextStatementTemplateTag(name: String) extends TemplateTag {
      override def extract(stream: TokenStream)(implicit compiler: SQLCompiler): SQLTemplateParams = {
        SQLTemplateParams(instructions = Map(name -> compiler.nextOpCodeOrDie(stream)))
      }

      override def toCode: String = s"%N:$name"
    }

    case class OptionalTemplateTag(tag: TemplateTag, dependents: List[OptionalDependentTemplateTag] = Nil) extends TemplateTag {
      override def extract(stream: TokenStream)(implicit compiler: SQLCompiler): SQLTemplateParams = {
        stream.mark()
        try {
          dependents.foldLeft(tag.extract(stream)) { (params, dependent) => params + dependent.extract(stream) }
        } catch {
          case _: Exception =>
            stream.rollback()
            SQLTemplateParams()
        }
      }

      override def toCode: String = s"?${tag.toCode} ${dependents.map(_.toCode).mkString(" ")}"
    }

    case class OptionalDependentTemplateTag(tag: TemplateTag) extends TemplateTag {
      override def extract(stream: TokenStream)(implicit compiler: SQLCompiler): SQLTemplateParams = {
        try tag.extract(stream) catch {
          case _: Exception => SQLTemplateParams()
        }
      }

      override def toCode: String = s"+?${tag.toCode}"
    }

    case class OptionalSequenceTemplateTag(options: List[TemplateTag] = Nil) extends TemplateTag {
      override def extract(stream: TokenStream)(implicit compiler: SQLCompiler): SQLTemplateParams = {
        var params: SQLTemplateParams = SQLTemplateParams()
        var position = 0
        do {
          position = stream.getPosition
          params = options.foldLeft(params) { case (params, option) => params + option.extract(stream) }
        } while (position != stream.getPosition)
        params
      }

      override def toCode: String = s"%O ${options.map(_.toCode).mkString("{{ ", " ", " }}")}"
    }

    case class OptionalMultiSequenceTemplateTag(options: List[TemplateTag] = Nil) extends TemplateTag {
      override def extract(stream: TokenStream)(implicit compiler: SQLCompiler): SQLTemplateParams = {
        var counter: Int = 0
        var repeatedSets: Map[String, List[SQLTemplateParams]] = Map.empty
        var position = 0
        do {
          position = stream.getPosition
          repeatedSets = options.foldLeft(repeatedSets) { case (bag, option) =>
            val newParams = option.extract(stream)
            if (newParams.isEmpty) bag else {
              counter += 1
              bag ++ Map(counter.toString -> List(newParams))
            }
          } ++ repeatedSets
        } while (position != stream.getPosition)
        SQLTemplateParams(repeatedSets = repeatedSets)
      }

      override def toCode: String = s"%OO ${options.map(_.toCode).mkString("{{ ", " ", " }}")}"
    }

    case class OrderedColumnsTemplateTag(name: String) extends TemplateTag {
      override def extract(stream: TokenStream)(implicit compiler: SQLCompiler): SQLTemplateParams = {
        SQLTemplateParams(orderedFields = Map(name -> compiler.nextOrderedColumns(stream)))
      }

      override def toCode: String = s"%o:$name"
    }

    case class QueryOrTableOrVariableTemplateTag(name: String) extends TemplateTag {
      override def extract(ts: TokenStream)(implicit compiler: SQLCompiler): SQLTemplateParams = {
        SQLTemplateParams(instructions = Map(name -> compiler.nextQueryOrTableOrVariableWithAlias(ts)))
      }

      override def toCode: String = s"%q:$name"
    }

    case class QueryOrVariableTemplateTag(name: String) extends TemplateTag {
      override def extract(ts: TokenStream)(implicit compiler: SQLCompiler): SQLTemplateParams = {
        SQLTemplateParams(instructions = Map(name -> compiler.nextQueryOrVariableWithAlias(ts)))
      }

      override def toCode: String = s"%Q:$name"
    }

    case class RegExTemplateTag(pattern: String) extends TemplateTag {
      override def extract(ts: TokenStream)(implicit compiler: SQLCompiler): SQLTemplateParams = {
        if (ts.isRegExMatch(pattern)) SQLTemplateParams() else ts.diePatternNotMatched()
      }

      override def toCode: String = s"%r:`$pattern`"
    }

    case class TableOrVariableTemplateTag(name: String) extends TemplateTag {
      override def extract(stream: TokenStream)(implicit compiler: SQLCompiler): SQLTemplateParams = {
        SQLTemplateParams(locations = Map(name -> compiler.nextTableOrVariable(stream)))
      }

      override def toCode: String = s"%L:$name"
    }

  }

}