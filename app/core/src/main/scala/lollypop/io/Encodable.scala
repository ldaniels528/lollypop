package lollypop.io

/**
 * Represents any object or value which can be encoded as bytes
 */
trait Encodable {

  def encode: Array[Byte]

}
