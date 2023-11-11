package com.lollypop.language

import com.lollypop.language.LollypopUniverse.{_classLoader, _dataTypeParsers, _helpers, _languageParsers}
import com.lollypop.language.instructions.Include
import com.lollypop.language.models._
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
import com.lollypop.util.ResourceHelper.AutoClose
import lollypop.io._
import lollypop.lang._

import java.io.{File, FileWriter, PrintWriter}

/**
 * Lollypop Universe - repository for long-lived state
 * @param dataTypeParsers the [[DataTypeParser data type parser]]
 * @param languageParsers the [[LanguageParser language parser]]
 * @param classLoader     the [[DynamicClassLoader classloader]]
 * @param escapeCharacter the [[Char escape character]]
 * @param isServerMode    indicates whether STDERR, STDIN and STDOUT is to be buffered
 */
case class LollypopUniverse(var dataTypeParsers: List[DataTypeParser] = _dataTypeParsers,
                            var languageParsers: List[LanguageParser] = _languageParsers,
                            var helpers: List[HelpIntegration] = _helpers,
                            var classLoader: DynamicClassLoader = _classLoader,
                            var escapeCharacter: Char = '`',
                            var isServerMode: Boolean = false) {
  // create the compiler and system utilities
  val compiler: LollypopCompiler = LollypopCompiler(this)
  val nodes = new Nodes(this)
  val system: OS = new OS(this)

  def createRootScope: () => Scope = {
    val rootScope = DefaultScope(universe = this)
      .withVariable(name = "__session__", value = this)
      .withVariable(name = "π", value = Math.PI)
      .withVariable("Nodes", value = nodes)
      .withVariable(name = "OS", value = system)
      .withVariable(name = "Random", value = lollypop.lang.Random)
      .withVariable(name = "stderr", value = system.stdErr.writer)
      .withVariable(name = "stdout", value = system.stdOut.writer)
      .withVariable(name = "stdin", value = system.stdIn.reader)
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

  def getInstructionChain(ts: TokenStream, host: Instruction)(implicit compiler: SQLCompiler): Option[Instruction] = matchInstruction(ts) {
    case parser: InstructionPostfixParser => parser.parseInstructionChain(ts, host)
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

  def getKeywords: List[String] = antiFunctionParsers.flatMap(_.help.collect { case c if c.name.forall(_.isLetter) => c.name })

  def helpDocs: List[HelpDoc] = {
    (MacroLanguageParser :: helpers).flatMap(_.help).sortBy(_.name)
  }

  def isFunctionCall(ts: TokenStream)(implicit compiler: SQLCompiler): Boolean = {
    !ts.isBackticks && !ts.isQuoted && functionCallParsers.exists(_.understands(ts))
  }

  def isInstruction(ts: TokenStream)(implicit compiler: SQLCompiler): Boolean = {
    !ts.isBackticks && !ts.isQuoted && (antiFunctionParsers.exists(_.understands(ts)) || MacroLanguageParser.understands(ts))
  }

  private def antiFunctionParsers: List[LanguageParser] = languageParsers.filterNot(_.isInstanceOf[FunctionCallParser])

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
    After, AllFields, AlterTable, Amp, AmpAmp, AND, AnonymousFunction, ApplyTo, ArrayExpression, As, Assert, Async, Avg,
    Bar, BarBar, Between, Betwixt, BooleanType,
    CodeBlock, ClassOf, CodecOf, ColonColon, ColonColonColon, Contains, Count, CountUnique, CreateExternalTable,
    CreateFunction, CreateIndex, CreateMacro, CreateProcedure, CreateTable, CreateType, CreateUniqueIndex, CreateView,
    DeclareClass, DeclarePackage, DeclareTable, DeclareView, DefineFunction, Delete, Describe, DefineImplicit, Destroy,
    Div, DoWhile, Drop,
    Each, ElementAt, EOL, EQ, Every, Exists, Expose,
    Feature, From,
    Graph, GreaterGreater, GreaterGreaterGreater, GroupBy, GT, GTE,
    HashTag, Having, Help, Http,
    IF, Iff, Import, ImportImplicitClass, Include, Infix, IN, InsertInto, InstructionChain, InterfacesOf, Intersect,
    Into, InvokeVirtualMethod, Is, IsCodecOf, IsDefined, IsJavaMember, IsNotNull, Isnt, IsNull,
    Join,
    LessLess, LessLessLess, Let, Limit, Literal, LT, LTE,
    Macro, Matches, Max, MembersOf, Min, Minus, MinusMinus, Monadic,
    Namespace, NEG, NEQ, New, Not, NotImplemented, NS, Null,
    ObjectOf, Once, OR, OrderBy,
    Pagination, Percent, PercentPercent, Plus, PlusPlus, ProcedureCall,
    Require, Reset, Return, RowsOfValues,
    ScaleTo, Scenario, Select, SetVariable, SetVariableExpression, SpreadOperator, Subtraction, Sum, SuperClassesOf,
    Synchronized, Switch,
    Table, TableLike, TableLiteral, TableZoo, This, ThrowException, Times, TimesTimes, Trace, TransferFrom, TransferTo,
    Transpose, TryCatch, Truncate, TupleLiteral, TypeOf,
    UnDelete, Union, Unique, UnNest, Up, Update, UpsertInto,
    ValVar, VariableRef, Verify,
    WhileDo, WhenEver, Where, WhereIn, With,
    ZipWith
  )
  val _helpers: List[HelpIntegration] = Nodes :: _languageParsers ::: _dataTypeParsers

  def overwriteOpCodesConfig(file: File): Unit = {
    new PrintWriter(new FileWriter(file)) use { out =>
      _languageParsers.foreach { lp =>
        out.println(lp.getClass.getTypeName)
      }
    }
  }

  RuntimePlatform.init()

}