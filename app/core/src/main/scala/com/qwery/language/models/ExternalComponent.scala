package com.qwery.language.models

/**
 * Represents an external JVM component
 * @param `class`     the JVM component's class name
 * @param jarLocation the optional Jar file path
 */
case class ExternalComponent(`class`: String, jarLocation: Option[String])
