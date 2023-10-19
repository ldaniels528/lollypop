package com.qwery

import com.qwery.runtime.RuntimeFiles.RecursiveFileList
import com.qwery.util.DateHelper
import org.slf4j.LoggerFactory

import java.io.File
import java.util.Date
import scala.language.implicitConversions

/**
 * Qwery database package object
 */
package object runtime extends AppConstants {
  private val logger = LoggerFactory.getLogger(getClass)

  type ROWID = Long

  //////////////////////////////////////////////////////////////////////////////////////
  //  SERVER CONFIG
  //////////////////////////////////////////////////////////////////////////////////////

  def getServerRootDirectory: File = {
    val directory = new File(sys.env.getOrElse("QWERY_DB", "qwery_db"))
    assert(directory.mkdirs() || directory.exists(), die(s"Could not create or find the data directory: ${directory.getAbsolutePath}")
    )
    directory
  }

  def getDatabaseRootDirectory(databaseName: String): File = getServerRootDirectory / databaseName

  /////////////////////////////////////////////////////////////////////////////////////////////////
  //      UTILITIES
  /////////////////////////////////////////////////////////////////////////////////////////////////

  def readable(value: Any): String = value match {
    case null => "null"
    case a: Array[_] =>
      val _type = a.getClass.getComponentType.getSimpleName
      a.map(readable).mkString(s"Array[${_type}](", ", ", ")")
    case d: Date => s"'${DateHelper.format(d)}'"
    case x => x.toString
  }

  /**
   * Returns the option of the given value as the desired type
   * @param value the given value
   * @tparam T the desired type
   * @return the option of the given value casted as the desired type
   */
  def safeCast[T](value: Any): Option[T] = value match {
    case null => None
    case v: T => Option(v)
    case x =>
      logger.warn(s"Failed to cast '$value' (${Option(x).map(_.getClass.getName).orNull})")
      Option.empty[T]
  }

  type StopWatch = () => Double

  def stopWatch: StopWatch = {
    val startTime = System.nanoTime()
    () => (System.nanoTime() - startTime) / 1e+6
  }

  /**
   * Executes the block capturing its execution the time in milliseconds
   * @param block the block to execute
   * @return a tuple containing the result of the block and its execution the time in milliseconds
   */
  def time[A](block: => A): (A, Double) = {
    val clock = stopWatch
    (block, clock())
  }

  /////////////////////////////////////////////////////////////////////////////////////////////////
  //      IMPLICIT CLASSES
  /////////////////////////////////////////////////////////////////////////////////////////////////

  final implicit class Boolean2Int(val bool: Boolean) extends AnyVal {
    def toInt: Int = if (bool) 1 else 0
  }

}
