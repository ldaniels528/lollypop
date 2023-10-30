package com.lollypop.language

/**
 * This is a [[LanguageParser]] that contains all built-in clauses (e.g. "if not exists")
 */
trait SQLLanguageParser extends LanguageParser
  with IfExists
  with IfNotExists
  with InsertValues
  with SetFieldValues
