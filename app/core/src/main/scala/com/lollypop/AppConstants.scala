package com.lollypop

/**
  * Application Constants
  * @author lawrence.daniels@gmail.com
  */
trait AppConstants {
  val lollypopSessionID = "lollypopid"

  val MAJOR_VERSION: Int = 0
  val MINOR_VERSION: Int = 1
  val MINI_VERSION: Int = 6
  val MICRO_VERSION: Int = 8

  val version = s"$MAJOR_VERSION.$MINOR_VERSION.$MINI_VERSION.$MICRO_VERSION"

  val DEFAULT_HOST = "localhost"
  val DEFAULT_PORT = "8233"
  val DEFAULT_DATABASE = "lollypop"
  val DEFAULT_SCHEMA = "public"
  
  val __cost__ = "__cost__"
  val __database__ = "__database__"
  val __ec__ = "__ec__"
  val __implicit_imports__ = "__implicit_imports__"
  val __imports__ = "__imports__"
  val __INSTRUCTION__ = "__INSTRUCTION__"
  val __keywords__ = "__keywords__"
  val __loaded__ = "__loaded__"
  val __namespace__ = "__namespace__"
  val __port__ = "__port__"
  val __resources__ = "__resources__"
  val __RETURNED__ = "__RETURNED__"
  val __schema__ = "__schema__"
  val __scope__ = "__scope__"
  val __secret_variables__ = "__secret_variables__"
  val __self__ = "__self__"
  val __session__ = "__session__"
  val __tableConversion__ = "__tableConversion__"
  val __userHome__ = "__userHome__"
  val __userName__ = "__userName__"
  val __version__ = "__version__"

  // row ID-related
  val ROWID_NAME = "__id"
  val SRC_ROWID_NAME = "__src_id"

  // default name for single-column queryables
  val singleColumnResultName = "result"

  // byte quantities
  val ONE_BYTE = 1
  val INT_BYTES = 4
  val LONG_BYTES = 8
  val ROW_ID_BYTES = 8
  val SHORT_BYTES = 2

  // ISO8601 date, UUID and other constants
  val DECIMAL_NUMBER_REGEX = "^[+-]?(\\d*\\.?\\d+|\\d+\\.?\\d*)$"
  val INTEGER_NUMBER_REGEX = "^[+-]?(\\d+)$"
  val ISO_8601_DATE_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"
  val ISO_8601_REGEX = """^\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d(\.\d+)?(([+-]\d\d:\d\d)|Z)?$"""
  val JSON_DATE_FORMAT: String = ISO_8601_DATE_FORMAT
  val UUID_REGEX = "^[0-9a-fA-F]{8}\\b-[0-9a-fA-F]{4}\\b-[0-9a-fA-F]{4}\\b-[0-9a-fA-F]{4}\\b-[0-9a-fA-F]{12}$"

  // database object types
  val EXTERNAL_TABLE_TYPE = "external table"
  val FUNCTION_TYPE = "function"
  val MACRO_TYPE = "macro"
  val PROCEDURE_TYPE = "procedure"
  val PHYSICAL_TABLE_TYPE = "table"
  val TABLE_INDEX_TYPE = "index"
  val USER_TYPE = "user type"
  val VIEW_TYPE = "view"

  val objectTypes: Seq[String] = Seq(
    EXTERNAL_TABLE_TYPE, FUNCTION_TYPE, MACRO_TYPE, PHYSICAL_TABLE_TYPE,
    PROCEDURE_TYPE, TABLE_INDEX_TYPE, USER_TYPE, VIEW_TYPE
  )

  val tableTypes: Seq[String] = Seq(
    EXTERNAL_TABLE_TYPE, PHYSICAL_TABLE_TYPE, TABLE_INDEX_TYPE, VIEW_TYPE
  )

}