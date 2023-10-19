package com.qwery.language

import com.qwery.language.HelpDoc.{CATEGORY_MISC, PARADIGM_IMPERATIVE}

/**
 * Represents an integrated help document
 * @param name        the name of the instruction
 * @param category    the category of the instruction
 * @param paradigm    the paradigm of the instruction
 * @param syntax      the syntax of the instruction
 * @param description the description of the instruction
 * @param example     an example of the instruction in-use.
 */
case class HelpDoc(name: String,
                   category: String = CATEGORY_MISC,
                   paradigm: String = PARADIGM_IMPERATIVE,
                   syntax: String,
                   description: String,
                   example: String)

object HelpDoc {
  // Declarative Paradigms
  val PARADIGM_DECLARATIVE = "Declarative"
  val PARADIGM_FUNCTIONAL = "Functional"
  val PARADIGM_REACTIVE = "Reactive"

  // Imperative Paradigms
  val PARADIGM_IMPERATIVE = "Procedural"
  val PARADIGM_OBJECT_ORIENTED = "Object-Oriented"

  // Categories
  val CATEGORY_MISC = "Miscellaneous"
  val CATEGORY_ASYNC_IO = "Asynchronous I/O"
  val CATEGORY_BRANCHING_OPS = "Branching Ops"
  val CATEGORY_CONTROL_FLOW = "Control Flow"
  val CATEGORY_DATAFRAME = "DataFrame"
  val CATEGORY_DISTRIBUTED = "Distributed Processing"
  val CATEGORY_PATTERN_MATCHING = "Pattern Matching"
  val CATEGORY_REFLECTION = "JVM and Reflection"
  val CATEGORY_SCIENCE = "Science and Mathematics"
  val CATEGORY_SESSION = "Scope and Session"
  val CATEGORY_SYNC_IO = "Synchronous I/O"
  val CATEGORY_SYSTEMS = "System Tools"
  val CATEGORY_TESTING = "Testing"

}