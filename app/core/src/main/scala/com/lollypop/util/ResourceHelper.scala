package com.lollypop.util

import org.slf4j.LoggerFactory

import scala.language.reflectiveCalls

/**
 * Resource Helper
 * @author lawrence.daniels@gmail.com
 */
object ResourceHelper {
  private val logger = LoggerFactory.getLogger(getClass)

  /**
   * Executes the block capturing the execution time
   * @param block the block to execute
   * @tparam T the result type
   * @return a tuple containing the result and execution time in milliseconds
   */
  def time[T](block: => T): (T, Double) = {
    val startTime = System.nanoTime()
    val result = block
    val elapsedTime = (System.nanoTime() - startTime).toDouble / 1e+6
    (result, elapsedTime)
  }

  /**
   * Automatically closes a resource after the completion of a code block
   */
  final implicit class AutoClose[T <: AutoCloseable](val resource: T) extends AnyVal {

    @inline
    def use[S](block: T => S): S = try block(resource) finally {
      try resource.close() catch {
        case e: Exception =>
          logger.error(s"Failure occurred while closing resource $resource", e)
      }
    }

  }

}