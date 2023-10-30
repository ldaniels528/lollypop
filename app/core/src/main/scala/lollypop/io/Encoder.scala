package lollypop.io

/**
 * Represents a data type encoder
 */
trait Encoder {

  /**
   * Encodes the given value into a byte array
   * @param value the [[Any value]] to encode
   * @return a byte array representing the encoded value
   */
  def encode(value: Any): Array[Byte]

}
