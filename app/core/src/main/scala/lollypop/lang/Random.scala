package lollypop.lang

import scala.util.{Random => r}

/**
 * Random Object Generator
 */
object Random {

  /**
   * Returns a random Double value
   * @return a random [[Double]]
   * @example Random.nextDouble()
   */
  def nextDouble(): Double = r.nextDouble()

  /**
   * Returns a random Double value
   * @param maxValue the maximum value
   * @return a random [[Double]]
   * @example Random.nextDouble()
   */
  def nextDouble(maxValue: Double): Double = nextDouble() * maxValue

  /**
   * Returns a random integer value
   * @return a random [[Int]]
   * @example Random.nextInt()
   */
  def nextInt(): Int = r.nextInt()

  /**
   * Returns a random integer value
   * @param maxValue the maximum value
   * @return a random [[Int]]
   * @example Random.nextInt()
   */
  def nextInt(maxValue: Int): Int = (nextDouble() * maxValue).toInt

  /**
   * Returns a random long integer value
   * @return a random [[Long]]
   * @example Random.nextLong()
   */
  def nextLong(): Long = r.nextLong()

  /**
   * Returns a random integer value
   * @param maxValue the maximum value
   * @return a random [[Int]]
   * @example Random.nextLong()
   */
  def nextLong(maxValue: Long): Long = (nextDouble() * maxValue).toLong

  /**
   * Returns a random string value
   * @return a random [[String]]
   * @example Random.nextString(['A' to 'z'], 8)
   */
  def nextString(chars: Array[Char], length: Int): String = {
    String.valueOf((0 until length).map(_ => chars(nextInt(chars.length))).toArray)
  }

  /**
   * Returns a random string value
   * @return a random [[String]]
   * @example Random.nextString(['A' to 'z'], 8)
   */
  def nextString(chars: Array[Char], length: Number): String = {
    String.valueOf((0 until length.intValue()).map(_ => chars(nextInt(chars.length))).toArray)
  }

  override def toString: String = "lollypop.lang.Random"

}
