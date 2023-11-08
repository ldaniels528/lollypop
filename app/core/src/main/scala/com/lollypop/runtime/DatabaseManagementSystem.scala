package com.lollypop.runtime

import com.lollypop.die
import com.lollypop.language.models._
import com.lollypop.language.{TokenStream, dieFunctionNotCompilable, dieNoSuchColumn, dieObjectAlreadyExists, dieObjectIsNotAUserFunction}
import com.lollypop.runtime.DatabaseObjectConfig._
import com.lollypop.runtime.LollypopVM.implicits.LollypopVMSQL
import com.lollypop.runtime.ResourceManager.RowCollectionTracker
import com.lollypop.runtime.RuntimeFiles.RecursiveFileList
import com.lollypop.runtime.datatypes._
import com.lollypop.runtime.devices.ProductCollection.toColumns
import com.lollypop.runtime.devices.RowCollection.dieNotSubTable
import com.lollypop.runtime.devices.TableColumn.implicits.{SQLToColumnConversion, TableColumnToSQLColumnConversion}
import com.lollypop.runtime.devices._
import com.lollypop.runtime.instructions.infrastructure.CreateExternalTable.ExternalTableDeclaration
import com.lollypop.runtime.instructions.infrastructure.Macro
import com.lollypop.runtime.instructions.queryables.AssumeQueryable.EnrichedAssumeQueryable
import com.lollypop.runtime.instructions.queryables.RuntimeQueryable.getQueryReferences
import com.lollypop.util.CodecHelper._
import com.lollypop.util.LogUtil
import com.lollypop.util.OptionHelper.OptionEnrichment
import com.lollypop.util.ResourceHelper._
import lollypop.io.IOCost
import org.slf4j.LoggerFactory

import java.io.{FileNotFoundException, RandomAccessFile}
import scala.concurrent.duration.{DurationInt, FiniteDuration}
import scala.reflect.ClassTag

/**
 * Database Management System - This class is responsible for managing the life-cycle of durable database objects.
 */
trait DatabaseManagementSystem {

  /**
   * Deletes temporary files
   * @param expirationTime the file [[FiniteDuration expiration time]]
   * @return the number of files deleted
   * @example {{{
   *  objectOf("com.lollypop.runtime.DatabaseManagementSystem").deleteTempFiles(Interval('5 minutes'))
   * }}}
   */
  def deleteTempFiles(expirationTime: FiniteDuration = 1.hours): Int = {
    val cutOffTime = System.currentTimeMillis() - expirationTime.toMillis
    val tempDir = getServerRootDirectory / "temp" / "temp"
    tempDir.streamFilesRecursively.count {
      case f if f.lastModified() <= cutOffTime => f.deleteRecursively()
      case _ => false
    }
  }

  /**
   * Drops an object by reference
   * @param ns       the [[DatabaseObjectRef database object reference]]
   * @param ifExists indicates whether an existence check before attempting to delete
   * @return the [[IOCost cost]] of the operation
   */
  def dropObject(ns: DatabaseObjectNS, ifExists: Boolean): IOCost = {
    if (!ifExists && !ns.configFile.exists()) die(s"Object '${ns.toSQL}' (${ns.configFile.getAbsolutePath}) does not exist")
    ns.columnName match {
      case Some(columnName) =>
        // remove the index from the config
        val cfg = ns.getConfig
        ns.writeConfig(config = cfg.copy(indices = cfg.indices.filterNot(_.indexedColumnName == columnName)))
        // delete the index files
        ns.indexFile.map(_.delete())
        ns.subTableDataFile.map(_.delete())
      case None =>
        ResourceManager.close(ns)
        ns.rootDirectory.deleteRecursively()
    }
    IOCost(destroyed = 1)
  }

  //////////////////////////////////////////////////////////////////////////////////////
  //  EXTERNAL TABLES
  //////////////////////////////////////////////////////////////////////////////////////

  /**
   * Creates a reference to an external table
   * @param ns          the [[DatabaseObjectNS database object namespace]]
   * @param declaration the [[ExternalTableDeclaration external table declaration]]
   * @return the [[IOCost cost]] of the operation
   */
  def createExternalTable(ns: DatabaseObjectNS, declaration: ExternalTableDeclaration): IOCost = {
    ns.createRoot()
    ns.writeConfig(config = DatabaseObjectConfig(
      columns = declaration.columns,
      partitions = declaration.partitions,
      externalTable = Some(declaration.config)
    ))
    IOCost(created = 1)
  }

  /**
   * Retrieves an external table by reference
   * @param ns the [[DatabaseObjectNS database object namespace]]
   * @return the [[RowCollection external table]]
   */
  def readExternalTable(ns: DatabaseObjectNS): RowCollection = {
    val cfg = ns.getConfig.externalTable || die(s"External configuration not found for $ns")
    val indexCfg = ns.getConfig.indices
    val device = new ExternalFilesTableRowCollection(cfg, host = FileRowCollection(ns))
    if (indexCfg.nonEmpty) MultiIndexRowCollection(device) else device
  }

  //////////////////////////////////////////////////////////////////////////////////////
  //  DURABLE FUNCTIONS
  //////////////////////////////////////////////////////////////////////////////////////

  /**
   * Creates a new durable function
   * @param ns          the [[DatabaseObjectNS]]
   * @param fx          the [[TypicalFunction function]] to persist
   * @param ifNotExists if true, the operation will not fail when the view exists
   * @param scope       the implicit [[Scope scope]]
   * @return the [[IOCost cost]] of the operation
   */
  def createDurableFunction(ns: DatabaseObjectNS, fx: TypicalFunction, ifNotExists: Boolean)(implicit scope: Scope): IOCost = {
    if (ns.configFile.exists() && !ifNotExists) dieObjectAlreadyExists(ns)
    else {
      // create the root directory
      ns.createRoot()

      // create the function configuration file
      val config = DatabaseObjectConfig(
        columns = fx.params.map(_.toTableColumn),
        functionConfig = Some(FunctionConfig(sql = fx.code.toSQL)))

      // write the function config
      ns.writeConfig(config)
      IOCost(created = 1)
    }
  }

  /**
   * Retrieves the durable function by reference
   * @param ns    the [[DatabaseObjectNS function reference]]
   * @param scope the implicit [[Scope scope]]
   * @return the [[TypicalFunction function]]
   */
  def readDurableFunction(ns: DatabaseObjectNS)(implicit scope: Scope): TypicalFunction = {
    val config = ns.getConfig
    val sql = config.functionConfig.map(_.sql) || dieObjectIsNotAUserFunction(ns)
    val code = scope.getCompiler.nextExpression(stream = TokenStream(sql), expr0 = None) || dieFunctionNotCompilable(ns)
    TypicalFunction(ns.name, params = config.columns.map(_.toColumn), code)
  }

  //////////////////////////////////////////////////////////////////////////////////////
  //  INDEXED TABLES
  //////////////////////////////////////////////////////////////////////////////////////

  /**
   * Creates a new index table
   * @param ns          the [[DatabaseObjectNS index namespace]]
   * @param ifNotExists prevents errors if the file already exists
   * @return the [[IOCost cost]] of building the index
   */
  def createIndex(ns: DatabaseObjectNS, ifNotExists: Boolean)(implicit scope: Scope): IOCost = {
    // load the owning table's configuration
    assert(ns.configFile.exists(), s"Table configuration file for '$ns' (${ns.configFile.getPath}) was not found")
    val config = ns.getConfig

    // if the index already exists ...
    val indexColumnName = ns.columnName || dieNotSubTable(ns)
    var indexMap = Map[String, HashIndexConfig](config.indices.map(i => i.indexedColumnName -> i): _*)
    if (indexMap.contains(indexColumnName)) {
      if (ifNotExists) IOCost() else die(s"Index '$ns' already exists")
    }

    // add the index entry to the host's configuration
    indexMap = indexMap ++ Map(indexColumnName -> HashIndexConfig(indexColumnName, isUnique = false))
    ns.writeConfig(config = config.copy(indices = indexMap.values.toList))

    // ensure the base table is fully realized - fixes issue with virtual tables
    scope.getRowCollection(ns.copy(columnName = None)).getLength

    // build the index
    val hashIndex = HashIndexRowCollection(ns)
    hashIndex.use(_.rebuild())
  }

  /**
   * Creates a new unique index table
   * @param ns          the [[DatabaseObjectNS index namespace]]
   * @param ifNotExists prevents errors if the file already exists
   * @return the [[IOCost cost]] of building the index
   */
  def createUniqueIndex(ns: DatabaseObjectNS, ifNotExists: Boolean): IOCost = {
    // load the owning table's configuration
    assert(ns.configFile.exists(), s"Table configuration file for '$ns' (${ns.configFile.getPath}) was not found")
    val config = ns.getConfig

    // if the index already exists ...
    val indexColumnName = ns.columnName || dieNotSubTable(ns)
    var indexMap = Map[String, HashIndexConfig](config.indices.map(i => i.indexedColumnName -> i): _*)
    if (indexMap.contains(indexColumnName)) {
      if (ifNotExists) IOCost() else die(s"Index '$ns' already exists")
    }

    // add the index entry to the host's configuration
    indexMap = indexMap ++ Map(indexColumnName -> HashIndexConfig(indexColumnName, isUnique = true))
    ns.writeConfig(config = config.copy(indices = indexMap.values.toList))

    // build the index
    HashIndexRowCollection(ns).use(_.rebuild())
  }

  //////////////////////////////////////////////////////////////////////////////////////
  //  MACROs
  //////////////////////////////////////////////////////////////////////////////////////

  /**
   * Creates a new macro
   * @param ns          the [[DatabaseObjectRef object reference]]
   * @param m           the [[Macro Macro]]
   * @param ifNotExists indicates whether an existence check before attempting to create
   * @return the [[IOCost cost]] of the operation
   */
  def createMACRO(ns: DatabaseObjectNS, m: Macro, ifNotExists: Boolean): IOCost = {
    if (ns.configFile.exists() && !ifNotExists) dieObjectAlreadyExists(ns)
    else {
      // create the root directory
      ns.createRoot()

      // create the macro configuration file
      val config = DatabaseObjectConfig(
        columns = Nil,
        macroConfig = Option(MacroConfig(sql = m.code.toSQL, template = m.template))
      )

      // write the macro config
      ns.writeConfig(config)
      IOCost(created = 1)
    }
  }

  /**
   * Reads a macro into memory
   * @param ns    the [[DatabaseObjectRef macro reference]]
   * @param scope the implicit [[Scope scope]]
   * @return the [[Macro macro]]
   */
  def readMACRO(ns: DatabaseObjectNS)(implicit scope: Scope): Macro = {
    val config = ns.getConfig
    val mc = config.macroConfig || die(s"Object '${ns.toSQL}' is not a macro")
    Macro(template = mc.template, code = scope.getCompiler.compile(mc.sql))
  }

  //////////////////////////////////////////////////////////////////////////////////////
  //  PROCEDURES
  //////////////////////////////////////////////////////////////////////////////////////

  /**
   * Creates a new procedure
   * @param ns          the procedure [[DatabaseObjectRef reference]]
   * @param procedure   the [[Procedure procedure]]
   * @param ifNotExists if true, the operation will not fail when the view exists
   * @param scope       the implicit [[Scope scope]]
   * @return the [[IOCost cost]] of the operation
   */
  def createProcedure(ns: DatabaseObjectNS, procedure: Procedure, ifNotExists: Boolean)(implicit scope: Scope): IOCost = {
    if (ns.configFile.exists() && !ifNotExists) dieObjectAlreadyExists(ns)
    else {
      // create the root directory
      ns.createRoot()

      // create the procedure configuration file
      val config = DatabaseObjectConfig(
        columns = procedure.params.map(_.toTableColumn),
        procedure = Some(ProcedureConfig(
          parameters = procedure.params.map(_.toParameter),
          sql = procedure.code.toSQL))
      )

      // write the procedure config
      ns.writeConfig(config)
      IOCost(created = 1)
    }
  }

  /**
   * Retrieves the procedure by reference
   * @param ns    the [[DatabaseObjectRef procedure reference]]
   * @param scope the implicit [[Scope scope]]
   * @return the [[Procedure procedure]]
   */
  def readProcedure(ns: DatabaseObjectNS)(implicit scope: Scope): Procedure = {
    val config = ns.getConfig
    val procedureConfig = config.procedure || die(s"Object '${ns.toSQL}' is not a procedure")
    val parameters = procedureConfig.parameters
    val sql = procedureConfig.sql
    Procedure(params = parameters, code = scope.getCompiler.compile(sql))
  }

  //////////////////////////////////////////////////////////////////////////////////////
  //  PHYSICAL TABLES
  //////////////////////////////////////////////////////////////////////////////////////

  /**
   * Creates a new physical table
   * @param ns          the [[DatabaseObjectNS database object namespace]]
   * @param tableType   the [[TableType table type]]
   * @param ifNotExists prevents failure if the table already exists
   * @return the [[IOCost cost]] of the operation
   *
   */
  def createPhysicalTable(ns: DatabaseObjectNS, tableType: TableType, ifNotExists: Boolean): IOCost = {
    if (ifNotExists & ns.configFile.exists()) IOCost()
    else {
      // create the root directory and host table
      ns.createRoot(ns.tableDataFile)

      // create the table configuration file
      val config = DatabaseObjectConfig(columns = tableType.columns, partitions = tableType.partitions)
      ns.writeConfig(config)

      // create the associated multi-tenant tables
      val guestFiles = config.columns.map(_.`type`).zipWithIndex.collect { case (_: TableType, index) => ns.getMultiTenantDataFile(index) }
      val isCreated = guestFiles.map(f => f.exists() | f.createNewFile())
      if (!isCreated.forall(_ == true)) {
        val logger = LoggerFactory.getLogger(getClass)
        guestFiles.zip(isCreated).filterNot(_._2).zipWithIndex.foreach { case ((file, _), index) =>
          logger.warn(f"${index + 1}. File '${file.getCanonicalPath}' was not created")
        }
      }

      // return the cost
      IOCost(created = 1)
    }
  }

  /**
   * Retrieves a physical table by reference
   * @param ns the [[DatabaseObjectNS object namespace]]
   * @return the [[RowCollection physical table]]
   */
  def readPhysicalTable(ns: DatabaseObjectNS): RowCollection = {
    val table = ns.getConfig match {
      // partitioned table?
      case cfg if cfg.partitions.nonEmpty =>
        new PartitionedRowCollection(ns,
          columns = cfg.columns,
          partitionColumnIndex = cfg.partitions
            .map(name => cfg.columns.indexWhere(_.name == name) match {
              case -1 => dieNoSuchColumn(name)
              case n => n
            }).headOption || ns.die("No partition columns"),
          build = { (partitionKey: Any) =>
            new FileRowCollection(ns, cfg.columns, new RandomAccessFile(ns.partitionFile(partitionKey), "rw"))
          })
      // external table?
      case cfg if cfg.externalTable.nonEmpty => readExternalTable(ns)
      case cfg =>
        var resource: RowCollection = LogicalTableRowCollection(ns)
        if (cfg.indices.nonEmpty) resource = MultiIndexRowCollection(resource)
        resource
    }
    table.track()
  }

  /**
   * Creates a [[Product]]-backed persistent collection
   * @tparam A the [[Product product type]]
   * @return a new [[ProductCollection persistent collection]]
   */
  def readProductCollection[A <: Product : ClassTag](ns: DatabaseObjectNS): ProductCollection[A] = {
    val (config, device) = ns.readTableAndConfig
    val (columns, _class) = toColumns[A]
    val deviceColumns = Map(config.columns.map(c => c.name -> c.`type`): _*)
    val productColumns = Map(columns.map(c => c.name -> c.`type`): _*)
    val missingColumns = deviceColumns.collect { case (name, _type) if !productColumns.contains(name) => name + ':' + _type }
    assert(missingColumns.isEmpty, s"Class ${_class.getName} does not contain columns: ${missingColumns.mkString(", ")}")
    new ProductCollection[A](device)
  }

  //////////////////////////////////////////////////////////////////////////////////////
  //  USER-DEFINED/DERIVED TYPES
  //////////////////////////////////////////////////////////////////////////////////////

  /**
   * Creates a new user-defined type
   * @param ns          the [[DatabaseObjectNS object namespace]]
   * @param udt         the [[ColumnType user-defined type]]
   * @param ifNotExists if true, the operation will not fail when the view exists
   * @param scope       the implicit [[Scope scope]]
   * @return the [[IOCost cost]] of the operation
   */
  def createUserType(ns: DatabaseObjectNS, udt: ColumnType, ifNotExists: Boolean)(implicit scope: Scope): IOCost = {
    if (ifNotExists && ns.configFile.exists()) IOCost()
    else {
      // create the root directory
      ns.createRoot()

      // create the table configuration file
      ns.writeConfig(DatabaseObjectConfig(
        columns = Nil,
        description = None,
        userDefinedType = Some(UserDefinedTypeConfig(udt.toSQL))
      ))
      IOCost(created = 1)
    }
  }

  /**
   * Retrieves a user-defined type by reference
   * @param ns    the [[DatabaseObjectNS type reference]]
   * @param scope the implicit [[Scope scope]]
   * @return the [[DataType]]
   */
  def readUserType(ns: DatabaseObjectNS)(implicit scope: Scope): DataType = {
    val entity = ResourceManager.getResourceOrElseUpdate(ns, {
      val config = ns.getConfig
      val columnTypeSQL = config.userDefinedType.map(_.sql) || die(s"Column details for type '$ns' are missing")
      try DataType.parse(columnTypeSQL) catch {
        case _: FileNotFoundException => die(s"Type '${ns.toSQL}' could not be found")
      }
    })

    entity match {
      case dataType: DataType => dataType
      case _ => ns.die(s"${ns.toSQL} is not a data type")
    }
  }

  //////////////////////////////////////////////////////////////////////////////////////
  //  VIRTUAL TABLES
  //////////////////////////////////////////////////////////////////////////////////////

  /**
   * Creates a new view (virtual table)
   * @param ns          the [[DatabaseObjectNS view reference]]
   * @param view        the [[View view]]
   * @param ifNotExists if true, no error will be generated if the view already exists
   * @param scope       the implicit [[Scope scope]]
   * @return the [[IOCost cost]] of the operation
   */
  def createVirtualTable(ns: DatabaseObjectNS, view: View, ifNotExists: Boolean)(implicit scope: Scope): IOCost = {
    //val (database, schema, name) = view.ns.expand
    val cost0 = if (ns.configFile.exists() && !ifNotExists) dieObjectAlreadyExists(ns)
    else {
      // create the root directory
      ns.createRoot()

      // create the virtual table configuration file
      val (_, cost0, result1) = view.query.toSQL.searchSQL(scope)

      // write the config
      val queryB64 = view.query.toSQL.getBytes().toBase64
      val config = DatabaseObjectConfig(columns = result1.columns, virtualTable = Some(VirtualTableConfig(queryB64)))
      ns.writeConfig(config)

      // materialize the view
      val cost1 = FileRowCollection(ns).use(_.insert(result1))
      cost0 ++ cost1
    }
    cost0 ++ IOCost(created = 1)
  }

  /**
   * Retrieves the virtual table by reference
   * @param ns    the [[DatabaseObjectRef virtual table reference]]
   * @param scope the implicit [[Scope scope]]
   * @return the [[RowCollection virtual table row collection]]
   */
  def readVirtualTable(ns: DatabaseObjectNS)(implicit scope: Scope): RowCollection = {
    // retrieve and compile the query
    val queryB64 = ns.getConfig.virtualTable.map(_.sql) || die(s"Object '$ns' is not a view")
    val query = new String(queryB64.fromBase64)
    val queryable = scope.getCompiler.compile(query).asQueryable

    // get all of the dependencies for this view
    val dependencies = getQueryReferences(queryable).map {
      case n: DatabaseObjectNS => n
      case d => d.toNS
    }

    // setup the virtual table
    val device = new VirtualTableRowCollection(queryable, host = FileRowCollection(ns), dependencies)
    device.rebuildIfUpdated()
    if (ns.getConfig.indices.nonEmpty) MultiIndexRowCollection(device) else device
  }

  def getUpdatedTime(ns: DatabaseObjectNS): Long = ns.tableDataFile.lastModified()

}

/**
 * Database Management System Companion
 */
object DatabaseManagementSystem extends DatabaseManagementSystem {
  private val search: String => String = _.replace("%", ".*")

  // remove temporary directories
  private val deletedFiles = deleteTempFiles()
  if (deletedFiles > 0) {
    LogUtil(this).info(s"Deleted $deletedFiles temporary file(s)")
  }

  /**
   * Pattern Search With Options
   * @param pattern the SQL-like pattern (e.g. "test%")
   */
  final implicit class PatternSearchWithOptions(val pattern: Option[String]) extends AnyVal {
    @inline def like(text: String): Boolean = pattern.isEmpty || pattern.map(search).exists(text.matches)
  }

}