package lollypop.lang

import com.lollypop.database.QueryResponse
import com.lollypop.database.QueryResponse.QueryResultConversion
import com.lollypop.language._
import com.lollypop.language.models.{Expression, FunctionCall, Instruction}
import com.lollypop.repl.ProcessRun
import com.lollypop.runtime.DatabaseManagementSystem.getServerRootDirectory
import com.lollypop.runtime.DatabaseObjectConfig.implicits.RichDatabaseEntityConfig
import com.lollypop.runtime.DatabaseObjectNS.{configExt, readConfig}
import com.lollypop.runtime.LollypopVM.rootScope
import com.lollypop.runtime.RuntimeFiles.RecursiveFileList
import com.lollypop.runtime._
import com.lollypop.runtime.datatypes.Inferences.fromValue
import com.lollypop.runtime.datatypes._
import com.lollypop.runtime.devices.RowCollectionZoo.createQueryResultTable
import com.lollypop.runtime.devices._
import com.lollypop.util.DateHelper
import lollypop.io.Encodable
import lollypop.lang.OS._
import org.apache.commons.io.IOUtils

import java.io._
import java.net.URL
import java.nio.Buffer
import scala.jdk.CollectionConverters.MapHasAsScala
import scala.sys.process.Process
import scala.util.{Failure, Success, Try}

/**
 * Operating System Utilities
 * @param ctx the [[LollypopUniverse]]
 */
class OS(ctx: LollypopUniverse) {

  //////////////////////////////////////////////////////////////////////////////////
  //    STANDARD ERROR / INPUT / OUTPUT
  //////////////////////////////////////////////////////////////////////////////////

  val stdErr: ConsoleIO = new StdOutput(ctx, System.err)

  val stdIn: ConsoleIO = new StdInput(ctx)

  val stdOut: ConsoleIO = new StdOutput(ctx, System.out)

  //////////////////////////////////////////////////////////////////////////////
  //    FILE / DIRECTORY METHODS
  //////////////////////////////////////////////////////////////////////////////

  def compile(sourceCode: String): Instruction = ctx.compiler.compile(sourceCode)

  def compile(file: File): Instruction = ctx.compiler.compile(file)

  def compile(in: InputStream): Instruction = ctx.compiler.compile(in)

  def copy(src: File, dest: File): Boolean = {
    // ensure the base destination directory exists
    if (dest.isDirectory && !dest.exists()) dest.mkdirs()

    def getRelativePath(base: String, file: String): String = file.drop(base.length) match {
      case s if s.startsWith(File.separator) => "." + s
      case s => s
    }

    // copy the structure
    val srcBase = src.getCanonicalPath
    src.streamFilesRecursively foreach { srcFile =>
      // determine the destination path of the current file
      val relativePath = getRelativePath(srcBase, srcFile.getCanonicalPath)
      val destFile = new File(dest, relativePath)

      // ensure the destination directory exists
      destFile.getParentFile ~> { file => if (!file.exists()) file.mkdirs() }

      // copy the file
      new FileInputStream(srcFile) use { in =>
        new FileOutputStream(destFile) use (IOUtils.copy(in, _))
      }
    }
    true
  }

  def copy(src: String, dest: String): Boolean = copy(new File(src), new File(dest))

  def delete(file: File): Boolean = delete(file, recursive = false)

  def delete(path: String): Boolean = delete(new File(path))

  def delete(file: File, recursive: Boolean): Boolean = {
    if (recursive) file.deleteRecursively() else file.delete()
  }

  def delete(path: String, recursive: Boolean): Boolean = delete(new File(path), recursive)

  def getReferencedEntities: RowCollection = {
    val out = createQueryResultTable(Seq(
      TableColumn(name = "namespace", `type` = StringType),
      TableColumn(name = "description", `type` = StringType)
    ))
    ResourceManager.getResourceMappings.foreach { case (ns, entity) =>
      out.insert(Map(
        "namespace" -> ns.toSQL,
        "description" -> (entity match {
          case fc: FunctionCall => s"Function${fc.args.map(_.toSQL).mkString("(", ", ", ")")}"
          case rc: RowCollection => s"Table${rc.columns.map(_.name).mkString("(", ", ", ")")}"
          case e: Expression => e.toSQL.limit(40)
          case x => x.getClass.getSimpleName
        })
      ).toRow(out))
    }
    out
  }

  def getResource(name: String): URL = {
    Option(ctx.classLoader.getResource(name)) || this.getClass.getResource(name)
  }

  def mkdir(directory: File): Unit = directory.mkdir()

  def mkdir(path: String): Unit = mkdir(new File(path))

  def mkdirs(directory: File): Unit = directory.mkdirs()

  def mkdirs(path: String): Unit = mkdirs(new File(path))

  //////////////////////////////////////////////////////////////////////////////
  //    UTILITY METHODS
  //////////////////////////////////////////////////////////////////////////////

  /**
   * Executes an operating system command
   * @param commandString the given [[String command]] to execute
   * @return the [[RowCollection]]
   * @example {{{ OS.exec('ls -al') }}}
   */
  def exec(commandString: String): RowCollection = {
    ProcessRun.invoke(commandString)(rootScope)._3
  }

  /**
   * Executes an operating system command
   * @param commandString the given [[String command]] to execute
   * @return the [[RowCollection]]
   * @example {{{ OS.execSystem('iostat 1 5') }}}
   */
  def execSystem(commandString: String): LazyList[String] = {
    Process(commandString).lazyLines.flatMap(_.split("\n"))
  }

  /**
   * Execute a SQL statement returning the results as a [[QueryResponse response]] object.
   * @param sql the SQL code (e.g. '''OS.execQL("select symbol: 'GMTQ', exchange: 'OTCBB', lastSale: 0.1111")''')
   * @return the [[QueryResponse]]
   */
  def execQL(sql: String): QueryResponse = OS.execQL(sql)(ctx.createRootScope())

  /**
   * Retrieves a collection of files matching a path expression
   * @param fileOrDirectory the [[File file or directory]]
   * @return a collection of files matching a path expression
   * @example {{{ from OS.listFiles(new File(".")) where lastModified < DateTime('2023-05-01T04:00:32.000Z') order by length desc }}}
   */
  def listFiles(fileOrDirectory: File): RowCollection = {
    generateFileList(fileOrDirectory, _.streamFiles)
  }

  /**
   * Retrieves a collection of files matching a path expression
   * @param fileOrDirectory the [[File file or directory]]
   * @param recursive       optionally indicates that the file listing should be recursive
   * @return a collection of files matching a path expression
   * @example {{{ from OS.listFiles(new File("."), true) where lastModified < DateTime('2023-05-01T04:00:32.000Z') limit 10 }}}
   */
  def listFiles(fileOrDirectory: File, recursive: Boolean): RowCollection = {
    generateFileList(fileOrDirectory, if (recursive) _.streamFilesRecursively else _.streamFiles)
  }

  /**
   * Retrieves a collection of files matching a path expression
   * @param path the [[String path expression]]
   * @return a collection of files matching a path expression
   * @example {{{ from OS.listFiles(".") where lastModified < DateTime('2023-05-01T04:00:32.000Z') order by length desc }}}
   */
  def listFiles(path: String): RowCollection = listFiles(new File(path))

  /**
   * Retrieves a collection of files matching a path expression
   * @param path      the [[String path expression]]
   * @param recursive optionally indicates that the file listing should be recursive
   * @return a collection of files matching a path expression
   * @example {{{ from OS.listFiles(".", true) where lastModified < DateTime('2023-05-01T04:00:32.000Z') limit 10 }}}
   */
  def listFiles(path: String, recursive: Boolean): RowCollection = listFiles(new File(path), recursive)

  /**
   * Returns the lines of a text file as a row set
   * @param file the [[File file]] to read
   * @return the [[RowCollection]]
   */
  def read(file: File): RowCollection = {
    val out = createQueryResultTable(columns = Seq(TableColumn(name = "line", `type` = StringType)))
    LazyRowCollection(out, new ReaderIterator(file).map(line => Map("line" -> line).toRow(out)))
  }

  /**
   * Returns the lines of a text file as a row set
   * @param path the path of the file to read
   * @return the [[RowCollection]]
   */
  def read(path: String): RowCollection = read(new File(path))

  def run(code: Instruction): Any = code.execute(Scope())._3

  override def toString: String = "lollypop.lang.OS"

}

object OS {
  private val search: String => String = _.replace("%", ".*")

  def createFileTable(): RowCollection = {
    createQueryResultTable(columns = Seq(
      TableColumn(name = "name", `type` = StringType),
      TableColumn(name = "canonicalPath", `type` = StringType),
      TableColumn(name = "lastModified", `type` = DateTimeType),
      TableColumn(name = "length", `type` = Int64Type),
      TableColumn(name = "isDirectory", `type` = BooleanType),
      TableColumn(name = "isFile", `type` = BooleanType),
      TableColumn(name = "isHidden", `type` = BooleanType)
    ))
  }

  /**
   * Execute a SQL statement returning the results as a [[QueryResponse]] object.
   * @param sql the SQL code
   * @return the [[QueryResponse]]
   */
  def execQL(sql: String)(implicit scope: Scope): QueryResponse = {
    val compiledCode = scope.getCompiler.compile(sql)
    val ns_? = compiledCode.extractReferences.lastOption.collect { case ref: DatabaseObjectRef => ref.toNS }
    val (_, _, result1) = compiledCode.execute(scope)
    result1.toQueryResponse(ns_?, limit = None)
  }

  def generateFileList(fileOrDirectory: File, f: File => LazyList[File]): RowCollection = {
    implicit val rc: RowCollection = createFileTable()
    LazyRowCollection(rc, rows = f(fileOrDirectory).map(toFileRow))
  }

  /**
   * Retrieves all database objects
   * @return a database table containing the results
   */
  def getDatabases: RowCollection = {
    implicit val out: RowCollection = createQueryResultTable(Seq(
      TableColumn(name = "database", `type` = StringType),
      TableColumn(name = "lastModifiedTime", `type` = DateTimeType)))
    LazyRowCollection(out, getServerRootDirectory.streamFiles map { file =>
      val row = Map("database" -> file.getName, "lastModifiedTime" -> DateHelper.from(file.lastModified()))
      row.toRow
    })
  }

  /**
   * Retrieves all database objects
   * @return a database table containing the results
   */
  def getDatabaseObjects: RowCollection = {
    implicit val out: RowCollection = createQueryResultTable(Seq(
      TableColumn(name = "database", `type` = StringType),
      TableColumn(name = "schema", `type` = StringType),
      TableColumn(name = "name", `type` = StringType),
      TableColumn(name = "type", `type` = StringType),
      TableColumn(name = "column", `type` = StringType),
      TableColumn(name = "lastModifiedTime", `type` = DateTimeType),
      TableColumn(name = "qname", `type` = StringType),
      TableColumn(name = "comment", `type` = StringType)))
    LazyRowCollection(out, getObjectConfigFiles() flatMap { configFile =>
      val database = configFile.getParentFile.getParentFile.getParentFile.getName
      val schema = configFile.getParentFile.getParentFile.getName
      val name = getNameWithoutExtension(configFile)
      val lastModified = DateHelper.from(configFile.lastModified())
      val ns = DatabaseObjectNS(database, schema, name)
      val (row0, indices) = Try(readConfig(ns, configFile)) match {
        case Success(config) =>
          val indices = config.indices.map(_.indexedColumnName)
          Map("type" -> getDatabaseEntityTypeName(config), "comment" -> (config.description || "")) -> indices
        case Failure(e) =>
          Map("type" -> "???", "comment" -> e.getMessage) -> Nil
      }

      val row = Map[String, Any](
        "database" -> database, "schema" -> schema, "name" -> name, "qname" -> s"$database.$schema.$name",
        "lastModifiedTime" -> lastModified
      ) ++ row0

      // generate a row for the tables and row(s) for the indices
      val rows = row.toRow :: (for {
        column <- indices
        newRow = (row ++ Map("qname" -> s"$database.$schema.$name#$column", "column" -> column, "type" -> TABLE_INDEX_TYPE)).toRow
      } yield newRow)
      rows
    })
  }

  /**
   * Retrieves the operating system's environment variables
   * @return a database table containing the results
   */
  def getEnv: RowCollection = {
    createQueryResultTable(
      columns = Seq(
        TableColumn("name", StringType),
        TableColumn("value", AnyType)
      ),
      data = System.getenv().asScala.toList.map { case (name, value) =>
        Map("name" -> name, "value" -> value)
      })
  }

  /**
   * Retrieves all database schemas
   * @return a database table containing the results
   */
  def getDatabaseSchemas: RowCollection = {
    implicit val out: RowCollection = createQueryResultTable(Seq(
      TableColumn(name = "database", `type` = StringType),
      TableColumn(name = "schema", `type` = StringType),
      TableColumn(name = "lastModifiedTime", `type` = DateTimeType)))
    LazyRowCollection(out, getServerRootDirectory.streamFiles.flatMap(f => Option(f.listFiles()).toList.flatMap(_.toList)) map { file =>
      Map(
        "database" -> file.getParentFile.getName,
        "schema" -> file.getName,
        "lastModifiedTime" -> DateHelper.from(file.lastModified())).toRow
    })
  }

  /**
   * Searches for columns by name
   * @return a database table containing the results
   */
  def getDatabaseColumns: RowCollection = {
    implicit val out: RowCollection = createQueryResultTable(Seq(
      TableColumn(name = "database", `type` = StringType),
      TableColumn(name = "schema", `type` = StringType),
      TableColumn(name = "name", `type` = StringType),
      TableColumn(name = "type", `type` = StringType),
      TableColumn(name = "columnName", `type` = StringType),
      TableColumn(name = "columnType", `type` = StringType),
      TableColumn(name = "columnTypeName", `type` = StringType),
      TableColumn(name = "JDBCType", `type` = Int32Type),
      TableColumn(name = "maxSizeInBytes", `type` = Int32Type),
      TableColumn(name = "defaultValue", `type` = StringType),
      TableColumn(name = "comment", `type` = StringType),
      TableColumn(name = "isNullable", `type` = BooleanType),
      TableColumn(name = "qname", `type` = StringType),
      TableColumn(name = "error", `type` = StringType)))
    LazyRowCollection(out, getObjectConfigFiles() flatMap { configFile =>
      val database = configFile.getParentFile.getParentFile.getParentFile.getName
      val schema = configFile.getParentFile.getParentFile.getName
      val name = getNameWithoutExtension(configFile)
      val ns = DatabaseObjectNS(database, schema, name)
      val rows = Try(readConfig(ns, configFile)) match {
        case Success(config) =>
          config.columns map { column =>
            val qName = s"$database.$schema.$name"
            Map(
              "database" -> database,
              "schema" -> schema,
              "name" -> name,
              "qname" -> qName,
              "type" -> getDatabaseEntityTypeName(config),
              "columnName" -> column.name,
              "columnType" -> column.`type`.toSQL,
              "columnTypeName" -> column.`type`.name,
              "comment" -> config.description.orNull,
              "defaultValue" -> column.defaultValue.map(_.toSQL).orNull,
              "JDBCType" -> column.`type`.getJDBCType,
              "maxSizeInBytes" -> column.`type`.maxSizeInBytes)
          }
        case Failure(e) =>
          List(Map("name" -> configFile.getName.dropRight(1 + configExt.length), "error" -> e.getMessage))
      }
      rows.map(_.toRow)
    })
  }

  /**
   * Returns the size (in bytes) of an object
   * @param value the object whose size is being measured
   * @return the size (in bytes) of an object or -1 if not a Lollypop object
   */
  def sizeOf(value: Any): Long = value.normalize match {
    case null => 0
    case b: Buffer => b.limit()
    case f: File => f.length()
    case s: String => s.length
    // collections
    case a: Array[_] => a.map(sizeOf).sum
    case b: BitArray => b.encode.limit()
    case m: QMap[_, _] => m.keys.map(sizeOf).sum + m.values.map(sizeOf).sum
    case r: RowCollection => r.sizeInBytes
    case s: Seq[_] => s.map(sizeOf).sum
    case s: Set[_] => s.toSeq.map(sizeOf).sum
    case e: Encodable => e.encode.length
    case v => fromValue(v).maxSizeInBytes
  }

  /**
   * Returns the database entity name
   * @param config the [[DatabaseObjectConfig]]
   * @return the database entity name (e.g. "table")
   */
  private def getDatabaseEntityTypeName(config: DatabaseObjectConfig): String = {
    config match {
      case cfg if cfg.isExternalTable => EXTERNAL_TABLE_TYPE
      case cfg if cfg.isFunction => FUNCTION_TYPE
      case cfg if cfg.isMACRO => MACRO_TYPE
      case cfg if cfg.isProcedure => PROCEDURE_TYPE
      case cfg if cfg.isUserType => USER_TYPE
      case cfg if cfg.isVirtualTable => VIEW_TYPE
      case _ => PHYSICAL_TABLE_TYPE
    }
  }

  private def getNameWithoutExtension(file: File): String = {
    import com.lollypop.language._
    import com.lollypop.runtime._
    file.getName ~> { name => name.indexOfOpt(".") map (index => name.substring(0, index)) getOrElse name }
  }

  private def getObjectConfigFiles(databasePattern: Option[String] = None,
                                   schemaPattern: Option[String] = None,
                                   namePattern: Option[String] = None): LazyList[File] = {
    getServerRootDirectory.streamFilesRecursively
      .filter(f => f.getName.toLowerCase.endsWith(configExt)
        && (databasePattern.isEmpty || databasePattern.like(f.getParentFile.getName))
        && (schemaPattern.isEmpty || schemaPattern.like(f.getParentFile.getParentFile.getName))
        && (namePattern.isEmpty || namePattern.like(f.getName)))
  }

  def toFileRow(file: File)(implicit out: RowCollection): Row = {
    Map(
      "name" -> file.getName,
      "canonicalPath" -> file.getCanonicalPath,
      "lastModified" -> file.lastModified(),
      "length" -> file.length(),
      "isDirectory" -> file.isDirectory,
      "isFile" -> file.isFile,
      "isHidden" -> file.isHidden
    ).toRow
  }

  sealed trait ConsoleIO {
    def asString(): String = {
      val bytes = buffer.toByteArray
      buffer.reset()
      new String(bytes)
    }

    def buffer: ByteArrayOutputStream

    def reader: BufferedReader

    def writer: PrintStream

    protected def createStreams(): (ByteArrayOutputStream, PrintStream, BufferedReader) = {
      val buffer = new ByteArrayOutputStream()
      val writer = new PrintStream(buffer)
      val reader = new BufferedReader(new InputStreamReader(new SimulatedInputStream(buffer)))
      (buffer, writer, reader)
    }
  }

  class ReaderIterator(file: File) extends Iterator[String] {
    private val reader = new BufferedReader(new FileReader(file))
    private val iter = reader.lines().iterator()
    private var hasMore = true

    override def hasNext: Boolean = {
      hasMore = iter.hasNext
      hasMore
    }

    override def next(): String = {
      val line = iter.next()
      if (!hasMore) reader.close()
      line
    }
  }

  class StdInput(ctx: LollypopUniverse) extends ConsoleIO {
    val (buffer, writer, reader0) = createStreams()
    private val reader1 = new BufferedReader(new InputStreamReader(System.in))

    override def reader: BufferedReader = if (ctx.isServerMode) reader0 else reader1
  }

  class StdOutput(ctx: LollypopUniverse, writer1: PrintStream) extends ConsoleIO {
    val (buffer, writer0, reader) = createStreams()

    override def writer: PrintStream = if (ctx.isServerMode) writer0 else writer1
  }

  private class SimulatedInputStream(baos: ByteArrayOutputStream) extends InputStream {
    private var buffer: List[Byte] = Nil

    override def read(): Int = {
      // read bytes from the buffer stream
      val bytes = baos.toByteArray
      baos.reset()

      // write the bytes as characters to the string buffer
      if (bytes.nonEmpty) buffer = buffer ::: bytes.toList

      // read the characters
      if (buffer.isEmpty) -1 else {
        val value = buffer.head
        buffer = buffer.tail
        value.toInt
      }
    }
  }

  /**
   * Pattern Search With Options
   * @param pattern the SQL-like pattern (e.g. "test%")
   */
  final implicit class PatternSearchWithOptions(val pattern: Option[String]) extends AnyVal {
    @inline def like(text: String): Boolean = pattern.isEmpty || pattern.map(search).exists(text.matches)
  }

}