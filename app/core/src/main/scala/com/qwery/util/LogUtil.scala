package com.qwery.util

import org.slf4j.{Logger, LoggerFactory}

import scala.collection.concurrent.TrieMap

object LogUtil {
  private val logs = TrieMap[String, Logger]()

  def apply(inst: Any): Logger = {
    val className = inst.getClass.getName
    logs.getOrElseUpdate(className, LoggerFactory.getLogger(className))
  }

}
