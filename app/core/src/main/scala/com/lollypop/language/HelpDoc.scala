package com.lollypop.language

import com.lollypop.language.HelpDoc.{CATEGORY_UNCLASSIFIED, PARADIGM_IMPERATIVE}

/**
 * Represents an integrated help document
 * @param name           the name of the instruction
 * @param category       the category of the instruction
 * @param paradigm       the paradigm of the instruction
 * @param syntax         the syntax of the instruction
 * @param description    the description of the instruction
 * @param example        an example of the instruction in-use.
 * @param featureTitle   an optional feature title
 * @param isExperimental indicates whether the referenced instruction is to be considered "experimental"
 *                       meaning not for general use.
 */
case class HelpDoc(name: String,
                   category: String = CATEGORY_UNCLASSIFIED,
                   paradigm: String = PARADIGM_IMPERATIVE,
                   syntax: String = "",
                   description: String,
                   example: String,
                   featureTitle: Option[String] = None,
                   isExperimental: Boolean = false)

object HelpDoc {
  // Declarative Paradigms
  val PARADIGM_DECLARATIVE = "Declarative"
  val PARADIGM_FUNCTIONAL = "Functional"
  val PARADIGM_REACTIVE = "Reactive"

  // Imperative Paradigms
  val PARADIGM_IMPERATIVE = "Procedural"
  val PARADIGM_OBJECT_ORIENTED = "Object-Oriented"

  // Categories
  val CATEGORY_AGG_SORT_OPS = "Aggregation and Sorting"
  val CATEGORY_CONCURRENCY = "Concurrency"
  val CATEGORY_FILTER_MATCH_OPS = "Filtering and Matching"
  val CATEGORY_CONTROL_FLOW = "Control Flow"
  val CATEGORY_DATAFRAMES_INFRA = "Dataframe Management"
  val CATEGORY_DATAFRAMES_IO = "Dataframe I/O"
  val CATEGORY_MACHINE_LEARNING = "Machine Learning"
  val CATEGORY_TRANSFORMATION = "Transformation"
  val CATEGORY_JVM_REFLECTION = "JVM and Reflection"
  val CATEGORY_REPL_TOOLS = "REPL Tools"
  val CATEGORY_SCOPE_SESSION = "Scope and Session"
  val CATEGORY_SYSTEM_TOOLS = "System Tools"
  val CATEGORY_TESTING = "Testing - Unit/Integration"
  val CATEGORY_UNCLASSIFIED = "Unclassified"

}