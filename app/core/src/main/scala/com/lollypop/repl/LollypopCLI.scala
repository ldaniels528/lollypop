package com.lollypop.repl

import com.lollypop.AppConstants._
import com.lollypop.database.server.LollypopChartGenerator
import com.lollypop.language.LollypopUniverse
import com.lollypop.language.LollypopUniverse.overwriteOpCodesConfig
import com.lollypop.repl.LollypopCLI.logger
import com.lollypop.repl.REPLTools.getResourceFile
import com.lollypop.runtime.LollypopVM.implicits.InstructionExtensions
import com.lollypop.runtime.RuntimeFiles.RecursiveFileList
import com.lollypop.runtime.datatypes._
import com.lollypop.runtime.devices.RecordCollectionZoo._
import com.lollypop.runtime.devices.RowCollectionZoo.ProductToRowCollection
import com.lollypop.runtime.devices.{Row, RowCollection, TableColumn}
import com.lollypop.runtime.instructions.expressions.GraphResult
import com.lollypop.runtime.instructions.queryables.TableRendering
import com.lollypop.runtime.plastics.RuntimeClass.implicits.RuntimeClassConstructorSugar
import com.lollypop.runtime.{DatabaseManagementSystem, DatabaseObjectNS, DatabaseObjectRef, LollypopCodeDebugger, LollypopCompiler, LollypopVM, Scope, getServerRootDirectory}
import com.lollypop.util.ConsoleReaderHelper.createInteractiveConsoleReader
import com.lollypop.util.OptionHelper.OptionEnrichment
import com.lollypop.util.ResourceHelper._
import com.lollypop.util.StringRenderHelper
import com.lollypop.util.StringRenderHelper.StringRenderer
import lollypop.io.IOCost
import org.jfree.chart.ChartPanel
import org.slf4j.LoggerFactory

import java.awt.Dimension
import java.io.{File, FileInputStream}
import java.util.Date
import javax.swing.JFrame
import scala.annotation.tailrec
import scala.util.{Failure, Properties, Success, Try}

object LollypopCLI extends LollypopCLI {
  private val logger = LoggerFactory.getLogger(getClass)

  /**
   * For commandline execution
   * @param args the commandline arguments
   */
  def main(args: Array[String]): Unit = {
    import scala.Console._
    Console.println(s"$RESET${GREEN}Q${CYAN}W${MAGENTA}E${RED}R${BLUE}Y$YELLOW SHELL v$version$RESET")
    Console.println()

    createOpCodesConfigIfNotExists()

    implicit val compiler: LollypopCompiler = LollypopCompiler()
    cli(args, scope0 = Scope(LollypopUniverse()))
    ()
  }

  private def createOpCodesConfigIfNotExists(): Unit = {
    // create the "opcodes.txt" file?
    val opCodesFile = getServerRootDirectory / "opcodes.txt"
    if (!opCodesFile.exists() || opCodesFile.length() == 0) {
      logger.info(s"creating '${opCodesFile.getPath}'...")
      overwriteOpCodesConfig(opCodesFile)
    }
  }

}

trait LollypopCLI extends LollypopCodeDebugger {
  private val stmtTextLength = 8192
  private val historyNS = DatabaseObjectRef(databaseName = Properties.userName, schemaName = "system", name = "history")
  private val historyTable = getHistoryTable(historyNS)

  def cli(args: Array[String], scope0: Scope, console: () => String = createInteractiveConsoleReader)(implicit compiler: LollypopCompiler): Scope = {
    // include the ~/.lollypoprc file (if it exists)
    val scope1 = loadResourceFile(scope0, executeQuery).withVariable("history", historyTable)

    // execute the command (or start interactive mode)
    args.toList match {
      // start interactive mode
      case Nil => interact(scope1, console)
      // run a script w/arguments
      case scriptName :: scriptArgs =>
        val (scope2, cost2, result2) = runScript(scope1, scriptName, scriptArgs)
        handleSuccess(scope2, cost2, result2)
    }
  }

  /**
   * Facilitates the client conversation
   */
  @tailrec
  final def interact(scope: Scope, console: () => String = createInteractiveConsoleReader)(implicit compiler: LollypopCompiler): Scope = {
    showPrompt(scope)
    readFromConsole(console) match {
      // ignore blank lines
      case "" => interact(scope, console)
      // quit the shell?
      case "exit" => scope
      // start debugger?
      case sql if sql startsWith "debug " =>
        val file = new File(sql.drop(6)).getCanonicalFile
        logger.info(s"Loading '${file.getAbsolutePath}' for debugging...")
        Try(stepThrough(file, console)) match {
          case Success(newScope) => interact(newScope, console)
          case Failure(e) => interact(handleError(e)(scope), console)
        }
      // execute a complete statement?
      case sql => interact(executeQuery(scope, sql), console)
    }
  }

  def runScript(scope: Scope, file: String, args: Seq[String] = Nil): (Scope, IOCost, Any) = {
    // setup the root scope
    val scope0 = scope.withVariable(name = "__args__", value = args)

    // load and run the script
    val scriptFile = new File(file)
    Console.println(s"Executing SQL file '${scriptFile.getAbsolutePath}'...")
    val code = new FileInputStream(scriptFile).use(scope0.getCompiler.compile)
    code.execute(scope0)
  }

  private def executeQuery(scope: Scope, sql: String): Scope = {
    import scala.Console._

    def printStreams(implicit aScope: Scope): Unit = {
      // print STDOUT
      val stdOut = aScope.getUniverse.system.stdOut.asString()
      if (stdOut.nonEmpty) Console.println(stdOut)
      // print STDERR
      val stdErr = aScope.getUniverse.system.stdErr.asString()
      if (stdErr.nonEmpty) Console.println(s"$YELLOW$stdErr$RESET")
    }

    @tailrec
    def getType(value: Any): String = value match {
      case Some(v) => getType(v)
      case null | None => ""
      case _: RowCollection => "DataFrame"
      case v => v.getClass.getJavaTypeName
    }

    val response: Scope = {
      try {
        val (colorA, colorB) = (BLUE, MAGENTA)
        Console.println(s"${colorA}Waiting for response... (sent $colorB${sql.length} bytes$colorA)$RESET")

        // execute the query
        val stmtTime = new Date()
        val ((scope1, cost1, result1), elapsedTime) = time(LollypopVM.executeSQL(scope, sql))

        // record the statement
        historyTable.insert(Map(
          "stmt_text" -> sql.replaceAll("\n", " ").take(stmtTextLength),
          "stmt_time" -> stmtTime,
          "stmt_msec" -> elapsedTime
        ).toRow(historyTable))

        // print the processing time
        val (unitQty, unitName) = friendlyTime(elapsedTime)
        val coll = Option(result1).collect { case r: RowCollection => r }.toList
        val statusLine = f"CPU time: $colorB$unitQty%.1f $unitName(s)$colorA" ::
          Option(result1).map(r => s"Type: $colorB${getType(r)}$colorA").toList :::
          coll.map(r => s"Columns: $colorB${r.columns.length}$colorA") :::
          coll.map(r => s"Rows: $colorB${r.getLength}$colorA") :::
          coll.map(r => s"Width: $colorB${r.recordSize} byte(s)$colorA")
        Console.println(f"$colorA${statusLine.mkString(" | ")}$RESET")

        // print STDOUT and STDERR
        printStreams(scope1)

        // process the returned scope
        handleSuccess(scope1, cost1, result1)

      } catch {
        case e: Exception =>
          // print STDOUT and STDERR
          printStreams(scope)

          // update the scope with the error
          handleError(e)(scope)
      }
    }
    response.reset()
  }

  private def loadResourceFile(scope: Scope, executeQuery: (Scope, String) => Scope): Scope = {
    val rcFile = new File(Properties.userHome, ".lollypoprc")
    getResourceFile(rcFile) map (executeQuery(scope, _)) getOrElse scope
  }

  private def getHistoryTable(historyNS: DatabaseObjectNS): RowCollection = {
    DatabaseManagementSystem.createPhysicalTable(historyNS, TableType(columns = Seq(
      TableColumn(name = "stmt_id", `type` = RowNumberType),
      TableColumn(name = "stmt_time", `type` = DateTimeType),
      TableColumn(name = "stmt_msec", `type` = Int64Type),
      TableColumn(name = "stmt_text", `type` = StringType(stmtTextLength))
    )), ifNotExists = true)
    DatabaseManagementSystem.readPhysicalTable(historyNS)
  }

  private def friendlyTime(msec: Double): (Double, String) = {
    val units = Seq("msec" -> 1000.0, "sec" -> 60.0, "min" -> 60.0, "hour" -> 24.0, "day" -> 365.25, "year" -> Double.MaxValue)
    var friendlyValue: Double = msec
    var unit = 0
    var changed = true
    while (unit < units.length && changed) {
      val (_, factor) = units(unit)
      changed = friendlyValue >= factor
      if (changed) {
        unit += 1
        friendlyValue /= factor
      }
    }
    val (friendlyName, _) = units(unit)
    friendlyValue -> friendlyName
  }

  private def handleError(e: Throwable)(implicit scope: Scope): Scope = {
    Console.err.println(e.getMessage)
    Console.println()
    scope.withThrowable(e)
  }

  private def handleSuccess(scope: Scope, cost: IOCost, result: Any): Scope = {
    val isTable = scope("__tableConversion__").contains(true)
    val isCost = cost == result
    if (cost.getUpdateCount > 0) {
      if (isTable) cost.toTable(scope).tabulate().foreach(Console.println) else Console.println(cost)
    }
    val resultA = if (isCost) null else result
    resultA match {
      case b: Array[Byte] => Console.println(StringRenderHelper.toByteArrayString(b, isPretty = false))
      case d: GraphResult => showChart(d)
      case m: Matrix if isTable => m.toTable(scope).tabulate().foreach(Console.println)
      case r: Row if isTable => r.toRowCollection.tabulate().foreach(Console.println)
      case r: Row => Console.println(StringRenderHelper.toRowString(r))
      case r: RowCollection => r.tabulate(limit = 1000).foreach(Console.println)
      case s: Scope => s.toRowCollection.tabulate().foreach(Console.println)
      case t: TableRendering if isTable => t.toTable(scope).tabulate().foreach(Console.println)
      case p: Product if isTable => p.toRowCollection.tabulate().foreach(Console.println)
      case p: Product => Console.println(StringRenderHelper.toProductString(p))
      case () => Console.println("Ok")
      case x => Console.println(x.render)
    }
    scope
  }

  private def showChart(d: GraphResult): JFrame = {
    val chart = LollypopChartGenerator.generate(d)
    val frame = new JFrame()
    frame.setPreferredSize(new Dimension(600, 400))
    frame.setContentPane(new ChartPanel(chart))
    frame.pack()
    frame.setVisible(true)
    frame.requestFocus()
    frame
  }

  private def readFromConsole(console: () => String): String = console().trim

  private def showPrompt(scope: Scope): Unit = {
    import scala.Console._
    val database: String = scope.getDatabase || DEFAULT_DATABASE
    val schema: String = scope.getSchema || DEFAULT_SCHEMA
    Console.print(s"$CYAN${Properties.userName}:/$database/$schema$RESET> ")
  }

}
