package com.lollypop.database
package jdbc

import com.lollypop.AppConstants._
import com.lollypop.database.jdbc.types.JDBCWrapper
import com.lollypop.language.LollypopUniverse
import com.lollypop.runtime.datatypes._
import com.lollypop.runtime.devices.TableColumn

import java.sql.{DatabaseMetaData, ResultSet, ResultSetMetaData, RowIdLifetime}
import scala.beans.{BeanProperty, BooleanBeanProperty}
import scala.util.Properties

/**
 * Lollypop Database Metadata
 * @param connection the [[JDBCConnection connection]]
 * @param URL        the JDBC Connection URL (e.g. "jdbc:lollypop://localhost:8233/securities")
 */
class JDBCDatabaseMetaData(@BeanProperty val connection: JDBCConnection, @BeanProperty val URL: String)
  extends DatabaseMetaData with JDBCWrapper {

  @BeanProperty val catalogSeparator: String = "."
  @BeanProperty val catalogTerm: String = "DATABASE"
  @BeanProperty val databaseMajorVersion: Int = MAJOR_VERSION
  @BeanProperty val databaseMinorVersion: Int = MINOR_VERSION
  @BeanProperty val databaseProductName: String = "Lollypop"
  @BeanProperty val databaseProductVersion: String = s"$MAJOR_VERSION.$MINOR_VERSION.$MINI_VERSION"
  @BeanProperty val defaultTransactionIsolation: Int = ResultSet.CONCUR_UPDATABLE
  @BeanProperty val driverMajorVersion: Int = MAJOR_VERSION
  @BeanProperty val driverMinorVersion: Int = MINOR_VERSION
  @BeanProperty val driverVersion: String = databaseProductVersion
  @BeanProperty val driverName: String = s"Lollypop v$databaseProductVersion"
  @BeanProperty val extraNameCharacters: String = ""
  @BeanProperty val numericFunctions: String =
    """|scaleTo
       |""".stripMargin.trim.replace("\n", " ")
  @BeanProperty val JDBCMajorVersion: Int = MAJOR_VERSION
  @BeanProperty val JDBCMinorVersion: Int = MINOR_VERSION
  @BeanProperty val identifierQuoteString: String = "`"
  @BeanProperty val maxBinaryLiteralLength: Int = 0
  @BeanProperty val maxCharLiteralLength: Int = 0
  @BeanProperty val maxColumnNameLength: Int = 128
  @BeanProperty val maxColumnsInGroupBy: Int = 1
  @BeanProperty val maxColumnsInIndex: Int = 1
  @BeanProperty val maxColumnsInOrderBy: Int = 1
  @BeanProperty val maxColumnsInSelect: Int = 0
  @BeanProperty val maxColumnsInTable: Int = 0
  @BeanProperty val maxConnections: Int = 0
  @BeanProperty val maxCursorNameLength: Int = 128
  @BeanProperty val maxIndexLength: Int = 0
  @BeanProperty val maxSchemaNameLength: Int = 128
  @BeanProperty val maxProcedureNameLength: Int = 128
  @BeanProperty val maxCatalogNameLength: Int = 128
  @BeanProperty val maxRowSize: Int = 0
  @BeanProperty val maxStatementLength: Int = 0
  @BeanProperty val maxStatements: Int = 0
  @BeanProperty val maxTableNameLength: Int = 128
  @BeanProperty val maxTablesInSelect: Int = 0
  @BeanProperty val maxUserNameLength: Int = 128
  @BeanProperty val procedureTerm: String = "procedure"
  @BooleanBeanProperty var readOnly: Boolean = false
  @BeanProperty val resultSetHoldability: Int = ResultSet.HOLD_CURSORS_OVER_COMMIT
  @BeanProperty val rowIdLifetime: RowIdLifetime = RowIdLifetime.ROWID_VALID_FOREVER
  @BeanProperty val schemaTerm: String = "SCHEMA"
  @BeanProperty val searchStringEscape: String = "\\"
  @BeanProperty val stringFunctions: String =
    """|String
       |""".stripMargin.trim.replace("\n", " ").replace("  ", " ")
  @BeanProperty val systemFunctions: String =
    """|cpuTime DateTime ns
       |""".stripMargin.trim.replace("\n", " ").replace("  ", " ")
  @BeanProperty val SQLKeywords: String = JDBCUniverse().getKeywords.mkString(" ")
  @BeanProperty var SQLStateType: Int = _
  @BeanProperty val timeDateFunctions: String =
    """|DateTime
       |""".stripMargin.trim.replace("\n", " ").replace("  ", " ")
  @BeanProperty val userName: String = Properties.userName

  override def allProceduresAreCallable(): Boolean = true

  override def allTablesAreSelectable(): Boolean = true

  override def generatedKeyAlwaysReturned(): Boolean = true

  override def nullsAreSortedHigh(): Boolean = true

  override def nullsAreSortedLow(): Boolean = !nullsAreSortedHigh()

  override def nullsAreSortedAtStart(): Boolean = false

  override def nullsAreSortedAtEnd(): Boolean = !nullsAreSortedAtStart()

  override def usesLocalFiles(): Boolean = true

  override def usesLocalFilePerTable(): Boolean = true

  override def supportsMixedCaseIdentifiers(): Boolean = true

  override def storesUpperCaseIdentifiers(): Boolean = false

  override def storesLowerCaseIdentifiers(): Boolean = false

  override def storesMixedCaseIdentifiers(): Boolean = true

  override def supportsMixedCaseQuotedIdentifiers(): Boolean = true

  override def storesUpperCaseQuotedIdentifiers(): Boolean = false

  override def storesLowerCaseQuotedIdentifiers(): Boolean = false

  override def storesMixedCaseQuotedIdentifiers(): Boolean = true

  override def supportsAlterTableWithAddColumn(): Boolean = true

  override def supportsAlterTableWithDropColumn(): Boolean = true

  override def supportsColumnAliasing(): Boolean = false

  override def nullPlusNonNullIsNull(): Boolean = true

  override def supportsConvert(): Boolean = true

  override def supportsConvert(fromType: Int, toType: Int): Boolean = {
    import java.sql.Types._
    (fromType, toType) match {
      case (VARCHAR, _) => true
      case (_, VARCHAR) => true
      case (a, b) if a == b => true
      case _ => false
    }
  }

  override def supportsTableCorrelationNames(): Boolean = true

  override def supportsDifferentTableCorrelationNames(): Boolean = true

  override def supportsExpressionsInOrderBy(): Boolean = false

  override def supportsOrderByUnrelated(): Boolean = false

  override def supportsGroupBy(): Boolean = true

  override def supportsGroupByUnrelated(): Boolean = false

  override def supportsGroupByBeyondSelect(): Boolean = false

  override def supportsLikeEscapeClause(): Boolean = true

  override def supportsMultipleResultSets(): Boolean = true

  override def supportsMultipleTransactions(): Boolean = false

  override def supportsNonNullableColumns(): Boolean = false

  override def supportsMinimumSQLGrammar(): Boolean = true

  override def supportsCoreSQLGrammar(): Boolean = false

  override def supportsExtendedSQLGrammar(): Boolean = false

  override def supportsANSI92EntryLevelSQL(): Boolean = false

  override def supportsANSI92IntermediateSQL(): Boolean = false

  override def supportsANSI92FullSQL(): Boolean = false

  override def supportsIntegrityEnhancementFacility(): Boolean = false

  override def supportsOuterJoins(): Boolean = false

  override def supportsFullOuterJoins(): Boolean = false

  override def supportsLimitedOuterJoins(): Boolean = false

  override def isCatalogAtStart: Boolean = true

  override def supportsSchemasInDataManipulation(): Boolean = true

  override def supportsSchemasInProcedureCalls(): Boolean = true

  override def supportsSchemasInTableDefinitions(): Boolean = true

  override def supportsSchemasInIndexDefinitions(): Boolean = true

  override def supportsSchemasInPrivilegeDefinitions(): Boolean = true

  override def supportsCatalogsInDataManipulation(): Boolean = true

  override def supportsCatalogsInProcedureCalls(): Boolean = true

  override def supportsCatalogsInTableDefinitions(): Boolean = true

  override def supportsCatalogsInIndexDefinitions(): Boolean = true

  override def supportsCatalogsInPrivilegeDefinitions(): Boolean = false

  override def supportsPositionedDelete(): Boolean = true

  override def supportsPositionedUpdate(): Boolean = true

  override def supportsSelectForUpdate(): Boolean = true

  override def supportsStoredProcedures(): Boolean = true

  override def supportsSubqueriesInComparisons(): Boolean = false

  override def supportsSubqueriesInExists(): Boolean = true

  override def supportsSubqueriesInIns(): Boolean = false

  override def supportsSubqueriesInQuantifieds(): Boolean = false

  override def supportsCorrelatedSubqueries(): Boolean = true

  override def supportsUnion(): Boolean = true

  override def supportsUnionAll(): Boolean = true

  override def supportsOpenCursorsAcrossCommit(): Boolean = false

  override def supportsOpenCursorsAcrossRollback(): Boolean = false

  override def supportsOpenStatementsAcrossCommit(): Boolean = false

  override def supportsOpenStatementsAcrossRollback(): Boolean = false

  override def doesMaxRowSizeIncludeBlobs(): Boolean = true

  override def supportsTransactions(): Boolean = false

  override def supportsTransactionIsolationLevel(level: Int): Boolean = false

  override def supportsDataDefinitionAndDataManipulationTransactions(): Boolean = false

  override def supportsDataManipulationTransactionsOnly(): Boolean = false

  override def dataDefinitionCausesTransactionCommit(): Boolean = false

  override def dataDefinitionIgnoredInTransactions(): Boolean = false

  override def supportsResultSetType(`type`: Int): Boolean = true

  override def supportsResultSetConcurrency(`type`: Int, concurrency: Int): Boolean = true

  override def ownUpdatesAreVisible(`type`: Int): Boolean = true

  override def ownDeletesAreVisible(`type`: Int): Boolean = true

  override def ownInsertsAreVisible(`type`: Int): Boolean = true

  override def othersUpdatesAreVisible(`type`: Int): Boolean = true

  override def othersDeletesAreVisible(`type`: Int): Boolean = true

  override def othersInsertsAreVisible(`type`: Int): Boolean = true

  override def updatesAreDetected(`type`: Int): Boolean = false

  override def deletesAreDetected(`type`: Int): Boolean = false

  override def insertsAreDetected(`type`: Int): Boolean = false

  override def supportsBatchUpdates(): Boolean = true

  override def supportsSavepoints(): Boolean = false

  override def supportsNamedParameters(): Boolean = false

  override def supportsMultipleOpenResults(): Boolean = true

  override def supportsGetGeneratedKeys(): Boolean = false

  override def supportsResultSetHoldability(holdability: Int): Boolean = true

  override def locatorsUpdateCopy(): Boolean = true

  override def supportsStatementPooling(): Boolean = false

  override def supportsStoredFunctionsUsingCallSyntax(): Boolean = true

  override def autoCommitFailureClosesAllResultSets(): Boolean = false

  override def getAttributes(catalog: String, schemaPattern: String, typeNamePattern: String, attributeNamePattern: String): ResultSet = {
    val columns = Seq(
      mkColumn(name = "TABLE_CAT", columnType = StringType),
      mkColumn(name = "TABLE_SCHEM", columnType = StringType),
      mkColumn(name = "TABLE_NAME", columnType = StringType),
      mkColumn(name = "ATTR_NAME", columnType = StringType),
      mkColumn(name = "DATA_TYPE", columnType = Int32Type),
      mkColumn(name = "ATTR_TYPE_NAME", columnType = StringType),
      mkColumn(name = "ATTR_SIZE", columnType = Int32Type),
      mkColumn(name = "DECIMAL_DIGITS", columnType = Int32Type),
      mkColumn(name = "NUM_PREC_RADIX", columnType = Int32Type),
      mkColumn(name = "NULLABLE", columnType = Int32Type),
      mkColumn(name = "REMARKS", columnType = StringType),
      mkColumn(name = "ATTR_DEF", columnType = StringType),
      mkColumn(name = "SQL_DATA_TYPE", columnType = Int32Type),
      mkColumn(name = "SQL_DATETIME_SUB", columnType = Int32Type),
      mkColumn(name = "CHAR_OCTET_LENGTH", columnType = Int32Type),
      mkColumn(name = "ORDINAL_POSITION", columnType = Int32Type),
      mkColumn(name = "IS_NULLABLE", columnType = StringType),
      mkColumn(name = "SCOPE_CATALOG", columnType = StringType),
      mkColumn(name = "SCOPE_SCHEMA", columnType = StringType),
      mkColumn(name = "SCOPE_TABLE", columnType = StringType),
      mkColumn(name = "SOURCE_DATA_TYPE", columnType = Int32Type),
      mkColumn(name = "IS_AUTOINCREMENT", columnType = StringType),
      mkColumn(name = "IS_GENERATEDCOLUMN", columnType = StringType))
    JDBCResultSet(connection, catalog, schemaPattern, tableName = "Attributes", columns, data = Nil)
  }

  override def getBestRowIdentifier(catalog: String, schema: String, table: String, scope: Int, nullable: Boolean): ResultSet = {
    val columns = Seq(
      mkColumn(name = "SCOPE", columnType = Int32Type),
      mkColumn(name = "COLUMN_NAME", columnType = StringType),
      mkColumn(name = "DATA_TYPE", columnType = Int32Type),
      mkColumn(name = "TYPE_NAME", columnType = StringType),
      mkColumn(name = "COLUMN_SIZE", columnType = Int32Type),
      mkColumn(name = "BUFFER_LENGTH", columnType = Int32Type),
      mkColumn(name = "DECIMAL_DIGITS", columnType = Int32Type),
      mkColumn(name = "PSEUDO_COLUMN", columnType = Int32Type))
    JDBCResultSet(connection, catalog, schema, tableName = "BestRowIdentifiers", columns, data = Seq(
      Seq(1, ROWID_NAME, Int32Type.getJDBCType, "Int", INT_BYTES, null, 0, 1)
    ))
  }

  override def getCatalogs: ResultSet = {
    JDBCResultSet(connection, connection.client.executeQuery(DEFAULT_DATABASE, DEFAULT_SCHEMA, sql =
      """|select TABLE_CAT: database from (OS.getDatabases())
         |""".stripMargin, limit = None))
  }

  override def getClientInfoProperties: ResultSet = {
    val columns = Seq(
      mkColumn(name = "NAME", columnType = StringType),
      mkColumn(name = "MAX_LEN", columnType = Int32Type),
      mkColumn(name = "DEFAULT_VALUE", columnType = StringType),
      mkColumn(name = "DESCRIPTION", columnType = StringType))
    JDBCResultSet(connection, DEFAULT_DATABASE, DEFAULT_SCHEMA, tableName = "ClientInfoProperties", columns, data = Seq(
      Seq("database", 1024, DEFAULT_DATABASE, "the name of the database"),
      Seq("schema", 1024, DEFAULT_SCHEMA, "the name of the schema"),
      Seq("server", 1024, DEFAULT_HOST, "the hostname of the server"),
      Seq("port", 5, DEFAULT_PORT, "the port of the server")
    ))
  }

  override def getColumns(catalog: String, schemaPattern: String, tableNamePattern: String, columnNamePattern: String): ResultSet = {
    JDBCResultSet(connection, connection.client.executeQuery(DEFAULT_DATABASE, DEFAULT_SCHEMA, sql =
      s"""|select
          |  TABLE_CAT: database,
          |  TABLE_SCHEM: schema,
          |  TABLE_NAME: name,
          |  COLUMN_NAME: columnName,
          |  DATA_TYPE: JDBCType,
          |  TYPE_NAME: columnTypeName,
          |  COLUMN_SIZE: maxSizeInBytes,
          |  BUFFER_LENGTH: 0,
          |  DECIMAL_DIGITS: 10,
          |  NUM_PREC_RADIX: 0,
          |  NULLABLE: iff(isNullable is true, 1, 0),
          |  REMARKS: comment,
          |  COLUMN_DEF: defaultValue,
          |  SQL_DATA_TYPE: 0,
          |  SQL_DATETIME_SUB: 0,
          |  CHAR_OCTET_LENGTH: 0,
          |  ORDINAL_POSITION: 0,
          |  IS_NULLABLE: iff(isNullable is true, 'YES', 'NO'),
          |  SCOPE_CATALOG: database,
          |  SCOPE_SCHEMA: schema,
          |  SCOPE_TABLE: name,
          |  SOURCE_DATA_TYPE: 0,
          |  IS_AUTOINCREMENT: 'NO',
          |  IS_GENERATEDCOLUMN: 'NO'
          |from (OS.getDatabaseColumns())
          |${where(catalog, schemaPattern, tableNamePattern, columnNamePattern)()}
          |""".stripMargin, limit = None))
  }

  override def getColumnPrivileges(catalog: String, schema: String, table: String, columnNamePattern: String): ResultSet = {
    JDBCResultSet(connection, connection.client.executeQuery(DEFAULT_DATABASE, DEFAULT_SCHEMA, sql =
      s"""|select
          |  database as TABLE_CAT,
          |  schema as TABLE_SCHEM,
          |  name as TABLE_NAME,
          |  columnName as COLUMN_NAME,
          |  'admin' as GRANTOR,
          |  'admin' as GRANTEE,
          |  'select, insert, update, delete' as PRIVILEGE,
          |  'NO' as IS_GRANTABLE
          |from OS.getDatabaseColumns()
          |${where(catalog, schema, table, columnNamePattern)()}
          |""".stripMargin, limit = None))
  }

  override def getCrossReference(parentCatalog: String, parentSchema: String, parentTable: String, foreignCatalog: String, foreignSchema: String, foreignTable: String): ResultSet = {
    val columns = Seq(
      mkColumn(name = "PKTABLE_CAT", columnType = StringType),
      mkColumn(name = "PKTABLE_SCHEM", columnType = StringType),
      mkColumn(name = "PKTABLE_NAME", columnType = StringType),
      mkColumn(name = "PKCOLUMN_NAME", columnType = StringType),
      mkColumn(name = "FKTABLE_CAT", columnType = StringType),
      mkColumn(name = "FKTABLE_SCHEM", columnType = StringType),
      mkColumn(name = "FKTABLE_NAME", columnType = StringType),
      mkColumn(name = "FKCOLUMN_NAME", columnType = StringType),
      mkColumn(name = "KEY_SEQ", columnType = Int32Type),
      mkColumn(name = "UPDATE_RULE", columnType = Int32Type),
      mkColumn(name = "DELETE_RULE", columnType = Int32Type),
      mkColumn(name = "FK_NAME", columnType = StringType),
      mkColumn(name = "PK_NAME", columnType = StringType),
      mkColumn(name = "DEFERRABILITY", columnType = Int32Type))
    JDBCResultSet(connection, parentCatalog, parentSchema, parentTable, columns, data = Nil)
  }

  override def getExportedKeys(catalog: String, schema: String, table: String): ResultSet = {
    val columns = Seq(
      mkColumn(name = "PKTABLE_CAT", columnType = StringType),
      mkColumn(name = "PKTABLE_SCHEM", columnType = StringType),
      mkColumn(name = "PKTABLE_NAME", columnType = StringType),
      mkColumn(name = "PKCOLUMN_NAME", columnType = StringType),
      mkColumn(name = "FKTABLE_CAT", columnType = StringType),
      mkColumn(name = "FKTABLE_SCHEM", columnType = StringType),
      mkColumn(name = "FKTABLE_NAME", columnType = StringType),
      mkColumn(name = "FKCOLUMN_NAME", columnType = StringType),
      mkColumn(name = "KEY_SEQ", columnType = Int32Type),
      mkColumn(name = "UPDATE_RULE", columnType = Int32Type),
      mkColumn(name = "DELETE_RULE", columnType = Int32Type),
      mkColumn(name = "FK_NAME", columnType = StringType),
      mkColumn(name = "PK_NAME", columnType = StringType),
      mkColumn(name = "DEFERRABILITY", columnType = Int32Type))
    JDBCResultSet(connection, catalog, schema, table, columns, data = Nil)
  }

  override def getFunctionColumns(catalog: String, schemaPattern: String, functionNamePattern: String, columnNamePattern: String): ResultSet = {
    JDBCResultSet(connection, connection.client.executeQuery(DEFAULT_DATABASE, DEFAULT_SCHEMA, sql =
      s"""|select
          |  database as FUNCTION_CAT,
          |  schema as FUNCTION_SCHEM,
          |  name as FUNCTION_NAME,
          |  columnName as COLUMN_NAME,
          |  columnType as COLUMN_TYPE,
          |  JDBCType as DATA_TYPE,
          |  columnType as TYPE_NAME,
          |  0 as PRECISION,
          |  sizeInBytes as LENGTH,
          |  0 as SCALE,
          |  0 as RADIX,
          |  if(isNullable is true, 1, 0) as NULLABLE,
          |  comment as REMARKS,
          |  0 as CHAR_OCTET_LENGTH,
          |  0 as ORDINAL_POSITION,
          |  if(isNullable is true, 'YES', 'NO') as IS_NULLABLE,
          |  qname as SPECIFIC_NAME
          |from OS.getDatabaseColumns()
          |${where(catalog, schemaPattern, functionNamePattern, columnNamePattern)(FUNCTION_TYPE)}
          |""".stripMargin, limit = None))
  }

  override def getFunctions(catalog: String, schemaPattern: String, functionNamePattern: String): ResultSet = {
    JDBCResultSet(connection, connection.client.executeQuery(DEFAULT_DATABASE, DEFAULT_SCHEMA, sql =
      s"""|select
          |  database as FUNCTION_CAT,
          |  schema as FUNCTION_SCHEM,
          |  name as FUNCTION_NAME,
          |  comment as REMARKS,
          |  'functionResultUnknown' as FUNCTION_TYPE,
          |  qname as SPECIFIC_NAME
          |from (OS.getDatabaseObjects())
          |${where(catalog, schemaPattern, functionNamePattern)(FUNCTION_TYPE)}
          |""".stripMargin, limit = None))
  }

  override def getImportedKeys(catalog: String, schema: String, table: String): ResultSet = {
    val columns = Seq(
      mkColumn(name = "PKTABLE_CAT", columnType = StringType),
      mkColumn(name = "PKTABLE_SCHEM", columnType = StringType),
      mkColumn(name = "PKTABLE_NAME", columnType = StringType),
      mkColumn(name = "PKCOLUMN_NAME", columnType = StringType),
      mkColumn(name = "FKTABLE_CAT", columnType = StringType),
      mkColumn(name = "FKTABLE_SCHEM", columnType = StringType),
      mkColumn(name = "FKTABLE_NAME", columnType = StringType),
      mkColumn(name = "FKCOLUMN_NAME", columnType = StringType),
      mkColumn(name = "KEY_SEQ", columnType = Int32Type),
      mkColumn(name = "UPDATE_RULE", columnType = Int32Type),
      mkColumn(name = "DELETE_RULE", columnType = Int32Type),
      mkColumn(name = "FK_NAME", columnType = StringType),
      mkColumn(name = "PK_NAME", columnType = StringType),
      mkColumn(name = "DEFERRABILITY", columnType = Int32Type))
    JDBCResultSet(connection, catalog, schema, tableName = "ImportedKeys", columns, data = Nil)
  }

  override def getIndexInfo(catalog: String, schema: String, table: String, unique: Boolean, approximate: Boolean): ResultSet = {
    JDBCResultSet(connection, connection.client.executeQuery(DEFAULT_DATABASE, DEFAULT_SCHEMA, sql =
      s"""|select
          |  TABLE_CAT: database,
          |  TABLE_SCHEM: schema,
          |  TABLE_NAME: name,
          |  NON_UNIQUE: true,
          |  INDEX_QUALIFIER: true,
          |  TYPE: 'tableIndexHashed',
          |  ORDINAL_POSITION: 0,
          |  COLUMN_NAME: column,
          |  ASC_OR_DESC: 'ascending',
          |  CARDINALITY: 0,
          |  PAGES: 1,
          |  FILTER_CONDITION: ''
          |from (OS.getDatabaseObjects())
          |${where(catalog, schema, table)(TABLE_INDEX_TYPE)}
          |""".stripMargin, limit = None))
  }

  override def getProcedures(catalog: String, schemaPattern: String, procedureNamePattern: String): ResultSet = {
    JDBCResultSet(connection, connection.client.executeQuery(DEFAULT_DATABASE, DEFAULT_SCHEMA, sql =
      s"""|select
          |  database as PROCEDURE_CAT,
          |  schema as PROCEDURE_SCHEM,
          |  name as PROCEDURE_NAME,
          |  comment as REMARKS,
          |  'procedureReturnsResult' as PROCEDURE_TYPE,
          |  qname as SPECIFIC_NAME
          |from (OS.getDatabaseObjects())
          |${where(catalog, schemaPattern, procedureNamePattern)(PROCEDURE_TYPE)}
          |""".stripMargin, limit = None))
  }

  override def getProcedureColumns(catalog: String, schemaPattern: String, procedureNamePattern: String, columnNamePattern: String): ResultSet = {
    JDBCResultSet(connection, connection.client.executeQuery(DEFAULT_DATABASE, DEFAULT_SCHEMA, sql =
      s"""|select
          |  database as PROCEDURE_CAT,
          |  schema as PROCEDURE_SCHEM,
          |  name as PROCEDURE_NAME,
          |  columnName as COLUMN_NAME,
          |  columnType as COLUMN_TYPE,
          |  JDBCType as DATA_TYPE,
          |  columnType as TYPE_NAME,
          |  0 as PRECISION,
          |  sizeInBytes as LENGTH,
          |  0 as SCALE,
          |  0 as RADIX,
          |  if(isNullable is true, 1, 0) as NULLABLE,
          |  comment as REMARKS,
          |  defaultValue as COLUMN_DEF,
          |  0 as SQL_DATA_TYPE,
          |  0 as SQL_DATETIME_SUB,
          |  0 as CHAR_OCTET_LENGTH,
          |  0 as ORDINAL_POSITION,
          |  if(isNullable is true, 'YES', 'NO') as IS_NULLABLE,
          |  qname as SPECIFIC_NAME
          |from OS.getDatabaseColumns()
          |${where(catalog, schemaPattern, procedureNamePattern, columnNamePattern)(PROCEDURE_TYPE)}
          |""".stripMargin, limit = None))
  }

  override def getSchemas: ResultSet = {
    JDBCResultSet(connection, connection.client.executeQuery(DEFAULT_DATABASE, DEFAULT_SCHEMA, sql =
      s"""|select
          |  schema as TABLE_SCHEM,
          |  database as TABLE_CATALOG
          |from (OS.getDatabaseSchemas())
          |${where(connection.catalog)()}
          |""".stripMargin, limit = None))
  }

  override def getSchemas(catalog: String, schemaPattern: String): ResultSet = {
    JDBCResultSet(connection, connection.client.executeQuery(DEFAULT_DATABASE, DEFAULT_SCHEMA, sql =
      s"""|select
          |  schema as TABLE_SCHEM,
          |  database as TABLE_CATALOG
          |from (OS.getDatabaseSchemas())
          |${where(catalog, schemaPattern)()}
          |""".stripMargin, limit = None))
  }

  override def getTables(catalog: String, schemaPattern: String, tableNamePattern: String, types: Array[String]): ResultSet = {
    val myTypes: Seq[String] = if (types == null || types.isEmpty) objectTypes else types.toSeq
    JDBCResultSet(connection, connection.client.executeQuery(DEFAULT_DATABASE, DEFAULT_SCHEMA, sql =
      s"""|select
          |  database as TABLE_CAT,
          |  schema as TABLE_SCHEM,
          |  name as TABLE_NAME,
          |  `type` as TABLE_TYPE,
          |  comment as REMARKS,
          |  database as TYPE_CAT,
          |  schema as TYPE_SCHEM,
          |  name as TYPE_NAME,
          |  null as SELF_REFERENCING_COL_NAME,
          |  null as REF_GENERATION
          |from (OS.getDatabaseObjects())
          |${where(catalog, schemaPattern, tableNamePattern)(myTypes: _*)}
          |""".stripMargin, limit = None))
  }

  override def getTypeInfo: ResultSet = {
    val types = LollypopUniverse._dataTypeParsers.collect { case dt: DataType => dt }
    val columns = Seq(
      mkColumn(name = "TYPE_NAME", columnType = StringType),
      mkColumn(name = "DATA_TYPE", columnType = Int32Type),
      mkColumn(name = "PRECISION", columnType = Int32Type),
      mkColumn(name = "LITERAL_PREFIX", columnType = StringType),
      mkColumn(name = "LITERAL_SUFFIX", columnType = StringType),
      mkColumn(name = "CREATE_PARAMS", columnType = StringType),
      mkColumn(name = "NULLABLE", columnType = Int32Type))
    JDBCResultSet(connection, DEFAULT_DATABASE, DEFAULT_SCHEMA, tableName = "Types", columns, data = types.map { columnType =>
      Seq(columnType.name, columnType.getJDBCType, columnType.precision, null, null, null, ResultSetMetaData.columnNullable)
    })
  }

  override def getTableTypes: ResultSet = {
    val columns = Seq(mkColumn(name = "TABLE_TYPE", columnType = StringType))
    JDBCResultSet(connection, DEFAULT_DATABASE, DEFAULT_SCHEMA, tableName = "TableTypes", columns, data = tableTypes map { tableType =>
      Seq(tableType)
    })
  }

  override def getTablePrivileges(catalog: String, schemaPattern: String, tableNamePattern: String): ResultSet = {
    JDBCResultSet(connection, connection.client.executeQuery(DEFAULT_DATABASE, DEFAULT_SCHEMA, sql =
      s"""|select
          |  TABLE_CAT: database,
          |  TABLE_SCHEM: schema,
          |  TABLE_NAME: name,
          |  GRANTOR: 'admin',
          |  GRANTEE: 'admin',
          |  PRIVILEGE: 'select, insert, update, delete',
          |  IS_GRANTABLE: 'NO'
          |from (OS.getDatabaseObjects())
          |${where(catalog, schemaPattern, tableNamePattern)(PHYSICAL_TABLE_TYPE)}
          |""".stripMargin, limit = None))
  }

  override def getPrimaryKeys(catalog: String, schema: String, table: String): ResultSet = {
    val columns = Seq(
      mkColumn(name = "TABLE_CAT", columnType = StringType),
      mkColumn(name = "TABLE_SCHEM", columnType = StringType),
      mkColumn(name = "TABLE_NAME", columnType = StringType),
      mkColumn(name = "COLUMN_NAME", columnType = StringType),
      mkColumn(name = "KEY_SEQ", columnType = Int32Type),
      mkColumn(name = "PK_NAME", columnType = StringType))
    JDBCResultSet(connection, catalog, schema, tableName = "PrimaryKeys", columns, data = Seq(
      Seq(catalog, schema, table, ROWID_NAME, 1, ROWID_NAME)
    ))
  }

  override def getPseudoColumns(catalog: String, schemaPattern: String, tableNamePattern: String, columnNamePattern: String): ResultSet = {
    val columns = Seq(
      mkColumn(name = "TABLE_CAT", columnType = StringType),
      mkColumn(name = "TABLE_SCHEM", columnType = StringType),
      mkColumn(name = "TABLE_NAME", columnType = StringType),
      mkColumn(name = "COLUMN_NAME", columnType = StringType),
      mkColumn(name = "DATA_TYPE", columnType = Int32Type),
      mkColumn(name = "COLUMN_SIZE", columnType = Int32Type),
      mkColumn(name = "DECIMAL_DIGITS", columnType = Int32Type),
      mkColumn(name = "NUM_PREC_RADIX", columnType = StringType),
      mkColumn(name = "COLUMN_USAGE", columnType = StringType),
      mkColumn(name = "REMARKS", columnType = StringType),
      mkColumn(name = "CHAR_OCTET_LENGTH", columnType = StringType),
      mkColumn(name = "IS_NULLABLE", columnType = StringType))
    JDBCResultSet(connection, DEFAULT_DATABASE, DEFAULT_SCHEMA, tableName = "PseudoColumns", columns, data = Nil)
  }

  override def getSuperTypes(catalog: String, schemaPattern: String, typeNamePattern: String): ResultSet = {
    val columns = Seq(
      mkColumn(name = "TABLE_CAT", columnType = StringType),
      mkColumn(name = "TABLE_SCHEM", columnType = StringType),
      mkColumn(name = "TABLE_NAME", columnType = StringType),
      mkColumn(name = "SUPERTYPE_CAT", columnType = StringType),
      mkColumn(name = "SUPERTYPE_SCHEM", columnType = StringType),
      mkColumn(name = "SUPERTABLE_NAME", columnType = StringType))
    JDBCResultSet(connection, DEFAULT_DATABASE, DEFAULT_SCHEMA, tableName = "SuperTypes", columns, data = Nil)
  }

  override def getSuperTables(catalog: String, schemaPattern: String, tableNamePattern: String): ResultSet = {
    val columns = Seq(
      mkColumn(name = "TABLE_CAT", columnType = StringType),
      mkColumn(name = "TABLE_SCHEM", columnType = StringType),
      mkColumn(name = "TABLE_NAME", columnType = StringType),
      mkColumn(name = "SUPERTABLE_NAME", columnType = StringType))
    JDBCResultSet(connection, DEFAULT_DATABASE, DEFAULT_SCHEMA, tableName = "SuperTables", columns, data = Nil)
  }

  override def getUDTs(catalog: String, schemaPattern: String, typeNamePattern: String, types: Array[Int]): ResultSet = {
    JDBCResultSet(connection, connection.client.executeQuery(DEFAULT_DATABASE, DEFAULT_SCHEMA, sql =
      s"""|select
          |  TABLE_CAT: database,
          |  TABLE_SCHEM: schema,
          |  TABLE_NAME: name,
          |  CLASS_NAME: sql,
          |  DATA_TYPE: null,
          |  REMARKS: comment,
          |  BASE_TYPE: null
          |from (OS.getDatabaseObjects())
          |${where(catalog, schemaPattern, typeNamePattern)(USER_TYPE)}
          |""".stripMargin, limit = None))
  }

  override def getVersionColumns(catalog: String, schema: String, table: String): ResultSet = {
    val columns = Seq(
      mkColumn(name = "SCOPE", columnType = Int32Type),
      mkColumn(name = "COLUMN_NAME", columnType = StringType),
      mkColumn(name = "DATA_TYPE", columnType = Int32Type),
      mkColumn(name = "TYPE_NAME", columnType = StringType),
      mkColumn(name = "COLUMN_SIZE", columnType = Int32Type),
      mkColumn(name = "BUFFER_LENGTH", columnType = StringType),
      mkColumn(name = "DECIMAL_DIGITS", columnType = Int32Type),
      mkColumn(name = "PSEUDO_COLUMN", columnType = Int32Type))
    JDBCResultSet(connection, catalog, schema, tableName = "VersionColumns", columns, data = Nil)
  }

  private def mkColumn(name: String, columnType: DataType) = {
    TableColumn(name = name, `type` = columnType)
  }

  private def where(catalog: String = null,
                    schema: String = null,
                    table: String = null,
                    columnNamePattern: String = null)(tableTypes: String*): String = {
    @inline
    def format(name: String, value: String): String = {
      s"$name ${if (value.contains("%")) "matches" else "is"} '${value.replace("%", ".*")}'"
    }

    val conditions =
      (if (tableTypes.nonEmpty) List(s"`type` in ${tableTypes.map(s => s"'$s'").mkString("[", ",", "]")}") else Nil) :::
        Option(catalog).map(format("database", _)).toList :::
        Option(schema).map(format("schema", _)).toList :::
        Option(table).map(format("name", _)).toList :::
        Option(columnNamePattern).map(format("columnName", _)).toList
    conditions.mkString("where ", " and ", "")
  }

}

object JDBCDatabaseMetaData {


}