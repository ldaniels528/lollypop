package lollypop.io

import java.nio.ByteBuffer

/**
 * Represents a data type decoder
 */
trait Decoder[T] {

  /**
   * Decodes the given byte buffer into typed [[T value]]
   * @param buf the [[ByteBuffer byte buffer]] to decode
   * @return the decoded [[T value]]
   */
  def decode(buf: ByteBuffer): T

}
