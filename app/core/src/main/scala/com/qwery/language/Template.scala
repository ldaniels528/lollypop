package com.qwery.language

import com.qwery.language.TemplateProcessor.tags._

import scala.language.implicitConversions

/**
 * Represents a template
 * @param tags the collection of template tags (e.g. "%c:condition")
 * @example {{{ "upsert into %L:target ?( +?%F:fields +?) %V:source where %c:condition" }}}
 */
case class Template(tags: List[TemplateTag]) {

  def matches(sql: String)(implicit compiler: SQLCompiler): Boolean = matches(TokenStream(sql))

  def matches(stream: TokenStream)(implicit compiler: SQLCompiler): Boolean = {
    stream.mark()
    try process(stream).nonEmpty catch {
      case _: Exception => false
    } finally stream.rollback()
  }

  def process(sql: String)(implicit compiler: SQLCompiler): SQLTemplateParams = process(TokenStream(sql))

  def process(stream: TokenStream)(implicit compiler: SQLCompiler): SQLTemplateParams = TemplateProcessor.execute(stream, tags)

  def processWithDebug(sql: String)(implicit compiler: SQLCompiler): SQLTemplateParams = processWithDebug(TokenStream(sql))

  def processWithDebug(stream: TokenStream)(implicit compiler: SQLCompiler): SQLTemplateParams = TemplateProcessor.executeWithDebug(stream, tags)

  override def toString: String = s"Template(\"${tags.map(_.toCode).mkString(" ")}\")"

}

object Template {

  def apply(templateString: String): Template = {
    new Template(tags = TemplateProcessor.compile(templateString))
  }

  def apply(tags: TemplateTag*): Template = new Template(tags = tags.toList)

}