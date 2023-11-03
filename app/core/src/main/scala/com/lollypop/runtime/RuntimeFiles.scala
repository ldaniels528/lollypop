package com.lollypop.runtime

import java.io.File
import scala.language.implicitConversions

/**
 * Runtime Files
 */
object RuntimeFiles {

  object implicits {

    implicit def string2File(string: String): File = new File(string)

  }

  /**
   * Recursive File List Enrichment
   * @param theFile the [[File file]]
   */
  final implicit class RecursiveFileList(val theFile: File) extends AnyVal {

    @inline def /(path: String) = new File(theFile, path)

    /**
     * Recursively deletes all files
     * @return true, if all files were deleted
     */
    def deleteRecursively(): Boolean = theFile match {
      case directory if directory.isDirectory => directory.listFiles().forall(_.deleteRecursively()) & directory.delete()
      case file => file.delete()
    }

    /**
     * Retrieves all files
     * @return the list of [[File files]]
     */
    def streamFiles: LazyList[File] = theFile.listFiles().to(LazyList)

    /**
     * Recursively retrieves all files
     * @return the list of [[File files]]
     */
    def streamFilesRecursively: LazyList[File] = theFile match {
      case directory if directory.isDirectory => directory.streamFiles.flatMap(_.streamFilesRecursively)
      case file => file #:: LazyList.empty
    }

  }

}