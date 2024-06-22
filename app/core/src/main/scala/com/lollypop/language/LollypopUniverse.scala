package com.lollypop.language

import com.lollypop.language.LollypopUniverse.{_classLoader, _dataTypeParsers, _languageParsers}
import com.lollypop.language.instructions.Include
import com.lollypop.language.models._
import com.lollypop.runtime.DatabaseManagementSystem._
import com.lollypop.runtime.RuntimeFiles.RecursiveFileList
import com.lollypop.runtime._
import com.lollypop.runtime.datatypes._
import com.lollypop.runtime.instructions.MacroLanguageParser
import com.lollypop.runtime.instructions.conditions._
import com.lollypop.runtime.instructions.expressions._
import com.lollypop.runtime.instructions.expressions.aggregation._
import com.lollypop.runtime.instructions.functions._
import com.lollypop.runtime.instructions.infrastructure._
import com.lollypop.runtime.instructions.invocables._
import com.lollypop.runtime.instructions.jvm._
import com.lollypop.runtime.instructions.operators._
import com.lollypop.runtime.instructions.queryables._
import com.lollypop.runtime.plastics.RuntimePlatform
import lollypop.io._
import lollypop.lang._
import org.slf4j.LoggerFactory

import java.io.{File, FileWriter, PrintWriter}
import scala.concurrent.ExecutionContext

/**
 * Lollypop Universe - repository for long-lived state
 * @param dataTypeParsers the [[DataTypeParser data type parser]]
 * @param languageParsers the [[LanguageParser language parser]]
 * @param classLoader     the [[DynamicClassLoader classloader]]
 * @param escapeCharacter the [[Char escape character]]
 * @param isServerMode    indicates whether STDERR, STDIN and STDOUT are to be buffered
 */
case class LollypopUniverse(var dataTypeParsers: List[DataTypeParser] = _dataTypeParsers,
                            var languageParsers: List[LanguageParser] = _languageParsers,
                            var classLoader: DynamicClassLoader = _classLoader,
                            var escapeCharacter: Char = '`',
                            var isServerMode: Boolean = false) {
  private lazy val logger = LoggerFactory.getLogger(getClass)
  private var f_debug: String => Unit = s => logger.debug(s)
  private var f_error: String => Unit = s => logger.error(s)
  private var f_info: String => Unit = s => logger.info(s)
  private var f_warn: String => Unit = s => logger.warn(s)

  // create the compiler and system utilities
  private var _helpers: List[HelpDoc] = Nil
  val compiler = new LollypopCompiler(this)
  val nodes = new Nodes(this)
  val system = new OS(this)

  def createRootScope: () => Scope = {
    val rootScope = DefaultScope(universe = this)
      .withVariable(name = __ec__, value = ExecutionContext.global)
      .withVariable(name = __session__, value = this)
      .withVariable(name = "π", value = Math.PI)
      .withVariable("Nodes", value = nodes)
      .withVariable(name = "OS", value = system)
      .withVariable(name = "Random", value = lollypop.lang.Random)
      .withVariable(name = "stderr", value = system.stdErr.writer)
      .withVariable(name = "stdout", value = system.stdOut.writer)
      .withVariable(name = "stdin", value = system.stdIn.reader)
      .withVariable("WebSockets", value = WebSockets)
      .withImports(Map(Seq(
        classOf[BitArray], classOf[Character], classOf[ComplexNumber], classOf[java.util.Date],
        classOf[IOCost], classOf[Matrix], classOf[Pointer], classOf[RowIDRange]
      ).map(c => c.getSimpleName -> c.getName): _*))
      .withVariable(name = "Character", value = classOf[Character])
    () => rootScope
  }

  /**
   * Returns a database entity by namespace
   * @param ns    the [[DatabaseObjectNS object namespace]]
   * @param scope the implicit [[Scope scope]]
   * @return the [[AnyRef referenced entity]]
   */
  def getReferencedEntity(ns: DatabaseObjectNS)(implicit scope: Scope): Any = {
    import DatabaseManagementSystem._
    import DatabaseObjectConfig.implicits.RichDatabaseEntityConfig
    scope.getMemoryObject(ns) match {
      case Some(entity) => entity
      case None =>
        ResourceManager.getResourceOrElseUpdate(ns, {
          ns.getConfig match {
            case config if config.isExternalTable => readExternalTable(ns)
            case config if config.isFunction => readDurableFunction(ns)
            case config if config.isMACRO => readMACRO(ns)
            case config if config.isProcedure => readProcedure(ns)
            case config if config.isUserType => readUserType(ns)
            case config if config.isVirtualTable => readVirtualTable(ns)
            case _ => readPhysicalTable(ns)
          }
        })
    }
  }

  //////////////////////////////////////////////////////////////////////////////
  //    OPCODES
  //////////////////////////////////////////////////////////////////////////////

  def createOpCodesConfig(ifNotExists: Boolean = true): Unit = {
    val opCodesFile = getServerRootDirectory / "opcodes.txt"
    if (!ifNotExists || (!opCodesFile.exists() || opCodesFile.length() == 0)) {
      this.info(s"creating '${opCodesFile.getPath}'...")
      overwriteOpCodesConfig(opCodesFile)
    }
  }

  private def overwriteOpCodesConfig(file: File): Unit = {
    new PrintWriter(new FileWriter(file)) use { out =>
      languageParsers.foreach { lp =>
        out.println(lp.getClass.getTypeName)
      }
    }
  }

  //////////////////////////////////////////////////////////////////////////////
  //    MACROS
  //////////////////////////////////////////////////////////////////////////////

  def addMacro(model: Macro): Unit = MacroLanguageParser.registerMacro(model)

  //////////////////////////////////////////////////////////////////////////////
  //    LANGUAGE PARSERS
  //////////////////////////////////////////////////////////////////////////////

  def functionCallParsers: List[FunctionCallParser] = languageParsers.collect { case fp: FunctionCallParser => fp }

  def getColumnType(ts: TokenStream)(implicit compiler: SQLCompiler): Option[ColumnType] = matchType(ts) {
    case parser: ColumnTypeParser => parser.parseColumnType(ts)
  }

  def getConditionToConditionChain(ts: TokenStream, host: Condition)(implicit compiler: SQLCompiler): Option[Condition] = matchInstruction(ts) {
    case parser: ConditionToConditionPostParser => parser.parseConditionChain(ts, host)
  }

  def getConditionToExpressionChain(ts: TokenStream, host: Condition)(implicit compiler: SQLCompiler): Option[Expression] = matchInstruction(ts) {
    case parser: ConditionToExpressionPostParser => parser.parseConditionChain(ts, host)
  }

  def getDirective(ts: TokenStream)(implicit compiler: SQLCompiler): Option[Instruction] = matchInstruction(ts) {
    case parser: DirectiveParser => parser.parseDirective(ts)
  }

  def getExpression(ts: TokenStream)(implicit compiler: SQLCompiler): Option[Expression] = matchInstruction(ts) {
    case parser: ExpressionParser => parser.parseExpression(ts)
    case parser: FunctionCallParser => parser.parseFunctionCall(ts)
  }

  def getExpressionChain(ts: TokenStream, host: Expression)(implicit compiler: SQLCompiler): Option[Expression] = matchInstruction(ts) {
    case parser: ExpressionChainParser => parser.parseExpressionChain(ts, host)
  }

  def getExpressionToConditionChain(ts: TokenStream, host: Expression)(implicit compiler: SQLCompiler): Option[Condition] = matchInstruction(ts) {
    case parser: ExpressionToConditionPostParser => parser.parseConditionChain(ts, host)
  }

  def getInstructionChain[A <: Instruction](ts: TokenStream, host: A)(implicit compiler: SQLCompiler): Option[Instruction] = matchInstruction(ts) {
    case parser: InstructionPostfixParser[A] => parser.parseInstructionChain(ts, host)
  }

  def getModifiable(ts: TokenStream)(implicit compiler: SQLCompiler): Option[Modifiable] = matchInstruction(ts) {
    case parser: ModifiableParser => parser.parseModifiable(ts)
  }

  def getQueryable(ts: TokenStream)(implicit compiler: SQLCompiler): Option[Queryable] = matchInstruction(ts) {
    case parser: QueryableParser => parser.parseQueryable(ts)
  }

  def getQueryableChain(ts: TokenStream, host: Queryable)(implicit compiler: SQLCompiler): Option[Queryable] = matchInstruction(ts) {
    case parser: QueryableChainParser => Option(parser.parseQueryableChain(ts, host))
  }

  def getFunctionCall(ts: TokenStream)(implicit compiler: SQLCompiler): Option[FunctionCall] = matchInstruction(ts) {
    case parser: FunctionCallParser => parser.parseFunctionCall(ts)
  }

  def getStatement(ts: TokenStream)(implicit compiler: SQLCompiler): Option[Instruction] = matchInstruction(ts) {
    case parser: InvokableParser => parser.parseInvokable(ts)
  }

  def getKeywords: List[String] = {
    antiFunctionParsers.flatMap(_.help.collect { case c if c.name.forall(_.isLetter) => c.name })
  }

  def helpDocs: List[HelpDoc] = {
    if (_helpers.isEmpty) {
      _helpers = (MacroLanguageParser.help ::: Nodes.help ::: dataTypeParsers.flatMap(_.help) :::
        languageParsers.flatMap(_.help)).distinct.sortBy(_.name)
    }
    _helpers
  }

  def isFunctionCall(ts: TokenStream)(implicit compiler: SQLCompiler): Boolean = {
    !ts.isBackticks && !ts.isQuoted && functionCallParsers.exists(_.understands(ts))
  }

  def isInstruction(ts: TokenStream)(implicit compiler: SQLCompiler): Boolean = {
    !ts.isBackticks && !ts.isQuoted && (antiFunctionParsers.exists(_.understands(ts)) || MacroLanguageParser.understands(ts))
  }

  def withLanguageParsers(lps: LanguageParser*): this.type = {
    this.languageParsers = (lps.toList ::: languageParsers).distinct
    this._helpers = Nil
    this
  }

  //////////////////////////////////////////////////////////////////////////////////
  //    Print I/O Streams
  //////////////////////////////////////////////////////////////////////////////////

  def debug(s: => String): this.type = {
    f_debug(s)
    this
  }

  def info(s: => String): this.type = {
    f_info(s)
    this
  }

  def warn(s: => String): this.type = {
    f_warn(s)
    this
  }

  def error(s: => String): this.type = {
    f_error(s)
    this
  }

  def withDebug(f: String => Unit): this.type = {
    f_debug = f
    this
  }

  def withError(f: String => Unit): this.type = {
    f_error = f
    this
  }

  def withInfo(f: String => Unit): this.type = {
    f_info = f
    this
  }

  def withWarn(f: String => Unit): this.type = {
    f_warn = f
    this
  }

  //////////////////////////////////////////////////////////////////////////////////
  //    Internal Utilities
  //////////////////////////////////////////////////////////////////////////////////

  private def antiFunctionParsers: List[LanguageParser] = {
    languageParsers.filterNot(_.isInstanceOf[FunctionCallParser])
  }

  private def matchType[A](ts: TokenStream)(f: PartialFunction[LanguageParser, Option[A]])(implicit compiler: SQLCompiler): Option[A] = {
    if (ts.isBackticks || ts.isQuoted) None else {
      for {
        instruction_? <- _dataTypeParsers.collectFirst { case parser: ColumnTypeParser if parser.understands(ts) & f.isDefinedAt(parser) => f(parser) }
        instruction <- instruction_?
      } yield instruction
    }
  }

  private def matchInstruction[A](ts: TokenStream)(f: PartialFunction[LanguageParser, Option[A]])(implicit compiler: SQLCompiler): Option[A] = {
    if (ts.isBackticks || ts.isQuoted) None else {
      for {
        instruction_? <- languageParsers.collectFirst { case parser if parser.understands(ts) & f.isDefinedAt(parser) => f(parser) }
        instruction <- instruction_?
      } yield instruction
    }
  }

}

/**
 * Compiler Context
 */
object LollypopUniverse {
  private val _classLoader = new DynamicClassLoader(classOf[LollypopUniverse].getClassLoader)
  val _dataTypeParsers: List[DataTypeParser] = List(
    // order is significant
    BitArrayType, BlobType, BooleanType, CharType, ClobType, DateTimeType, EnumType, Int8Type, Int16Type, Int32Type,
    Int64Type, Float32Type, Float64Type, NumericType, DurationType, JsonType, MatrixType, PointerType, RowIDRangeType,
    RowNumberType, SQLXMLType, TableType, UserDefinedDataType, UUIDType, StringType, VarBinaryType, VarCharType, AnyType
  )
  val _languageParsers: List[LanguageParser] = List[LanguageParser](
    After, AllFields, AlterTable, Amp, AmpAmp, AND, AnonymousFunction, ApplyTo, ArrayLiteral, As, Assert, Async,
    DecomposeTable, Avg,
    Bang, Bar, BarBar, Between, Betwixt, BooleanType,
    ClassPath, CodeBlock, ClassOf, CodecOf, ColonColon, ColonColonColon, Contains, Count, CountUnique, CreateExternalTable,
    CreateFunction, CreateIndex, CreateMacro, CreateProcedure, CreateTable, CreateType, CreateUniqueIndex, CreateView,
    DeclareClass, DeclarePackage, DeclareTable, DeclareView, Def, Delete, Describe, DefineImplicit, Destroy,
    Div, DoWhile, Drop,
    Each, ElementAt, EOL, EQ, Every, Exists, Expose,
    Feature, From,
    Graph, GreaterGreater, GreaterGreaterGreater, GroupBy, GT, GTE,
    HashTag, Having, Help, Histogram,
    IF, Iff, Import, ImportImplicitClass, Include, Infix, IN, InsertInto, InstructionChain, InterfacesOf, Intersect,
    Into, InvokeVirtualMethod, Is, IsCodecOf, IsDefined, IsJavaMember, Isnt,
    Join,
    LessLess, LessLessLess, Let, Limit, Literal, LollypopComponents, LT, LTE,
    Macro, Matches, Max, MembersOf, Min, Minus, MinusMinus, Monadic,
    Namespace, NEG, NEQ, New, NodePs, Not, NotImplemented, NS, Null,
    ObjectOf, Once, OR, OrderBy,
    Pagination, Percent, PercentPercent, Plus, PlusPlus, ProcedureCall, ProcessExec,
    Require, Reset, Return, RowsOfValues,
    ScaleTo, Scenario, Select, SetVariable, SetVariableExpression, Span, SpreadOperator, Subtraction, Sum, SuperClassesOf,
    Synchronized, Switch,
    Table, TableArray, TableLike, TableLiteral, TableZoo, This, ThrowException, Times, TimesTimes, Trace,
    TransferFrom, TransferTo, Transpose, TryCatch, Truncate, TupleLiteral, TypeOf,
    UnDelete, Union, Unique, UnNest, Up, Update, UpsertInto,
    ValVar, VariableRef, Verify,
    WhileDo, WhenEver, Where, WhereIn, With, WWW,
    ZipWith
  )

  RuntimePlatform.init()

}