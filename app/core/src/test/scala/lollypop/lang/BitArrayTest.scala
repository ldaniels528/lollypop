package lollypop.lang

import org.scalatest.funspec.AnyFunSpec

class BitArrayTest extends AnyFunSpec {

  describe(classOf[BitArray].getSimpleName) {

    it("should compute the required array length for a maximum value") {
      assert(BitArray.computeArraySize(129) == 3)
    }

    it("should support headOption") {
      val bitArray = BitArray(3, 6, 9)
      assert(bitArray.headOption contains 3)
    }

    it("should support lastOption") {
      val bitArray = BitArray(3, 6, 9)
      assert(bitArray.lastOption contains 9)
    }

    it("should support ++ for combining two bit arrays") {
      val bitArrayA = BitArray(3, 6, 9)
      val bitArrayB = BitArray(2, 5, 7)
      val bitArrayC = bitArrayA ++ bitArrayB
      assert(bitArrayC.ascending == List(2, 3, 5, 6, 7, 9))
    }

    it("should support add(value) and contains(value)") {
      val bitArray = BitArray(3, 6, 9)
      for (n <- 0 until 10) {
        assert(if (n > 0 && n % 3 == 0) bitArray.contains(n) else !bitArray.contains(n))
      }
    }

    it("should indicate the current allocated limits of the bit array") {
      val bitArray = BitArray(3, 6, 9)
      assert(bitArray.limits == (0, 63))
    }

    it("should resize itself if a value is too large") {
      val bitArray = BitArray.withRange(100, 108)
      bitArray.add(100, 103, 107)
      assert(bitArray.limits == (100, 163))
      assert(bitArray.ascending == Seq(100, 103, 107))
      assert(bitArray.encode.limit() == 20)

      bitArray.add(170, 200)
      assert(bitArray.contains(170))
      assert(bitArray.limits == (100, 227))
      assert(bitArray.ascending == Seq(100, 103, 107, 170, 200))
      assert(bitArray.encode.limit() == 28)
    }

    it("should resize itself if a value is too small (lower than the floor)") {
      val bitArray = BitArray.withRange(100, 108)
      bitArray.add(100, 101, 107)
      assert(bitArray.limits == (100, 163))
      assert(bitArray.ascending == Seq(100, 101, 107))
      assert(bitArray.encode.limit() == 20)

      bitArray.add(-1, 13)
      assert(bitArray.limits == (-1, 126))
      assert(bitArray.ascending == Seq(-1, 13, 100, 101, 107))
      assert(bitArray.encode.limit() == 28)
    }

    it("should return the value of the bit at an index within is set") {
      val bitArray = BitArray(3, 6, 9)
      assert(bitArray(3) == 1)
    }

    it("should test whether a bit at an index within is set") {
      val bitArray = BitArray(3, 6, 9)
      assert(bitArray.isSet(3) && !bitArray.isSet(4))
    }

    it("should return a ascending Seq of values") {
      val bitArray = BitArray(95, 64, 32)
      assert(bitArray.ascending == Seq(32, 64, 95))
    }

    it("should return a descending Seq of values") {
      val bitArray = BitArray(32, 64, 95)
      assert(bitArray.descending == Seq(95, 64, 32))
    }

    it("should indicate the presence or absence of values") {
      val bitArray = BitArray()
      assert(bitArray.isEmpty)

      bitArray.add(3)
      assert(bitArray.nonEmpty)
    }

    it("should return smallest (min) value from the bit array") {
      val bitArray = BitArray()
      assert(bitArray.min.isEmpty)

      bitArray.add(32, 64, 13, 95)
      assert(bitArray.min contains 13)
    }

    it("should return largest (max) value from the bit array") {
      val bitArray = BitArray()
      assert(bitArray.max.isEmpty)

      bitArray.add(32, 95, 64, 13)
      assert(bitArray.max contains 95)
    }

    it("should count() entries that match a condition") {
      val bitArray = BitArray(0, 17, 32, 61, 64, 95, 107, 122, 128)
      assert(bitArray.count(_ < 100) == 6)
    }

    it("should truncate() entries starting with a specific value") {
      val bitArray = BitArray(1, 2, 3, 5, 7, 11, 13, 19, 23)
      bitArray.truncate(7)
      assert(bitArray.ascending == Seq(1, 2, 3, 5))
    }

    it("should support traversal via foreach()") {
      val bitArray = BitArray(32, 64, 80, 16)

      var total = 0L
      bitArray.foreach(total += _)
      assert(total == 192L)
    }

    it("should support transformations via map()") {
      val bitArray = BitArray(32, 64, 95)
      assert(bitArray.map(_ / 2) == Seq(16, 32, 47))
    }

    it("should support replaceAll(values: _*)") {
      val bitArray = BitArray(17, 61, 95, 122)
      assert(bitArray.ascending == Seq(17, 61, 95, 122))
      assert(bitArray.floor == 0)

      bitArray.replaceAll(77, 111, 32, 45, 66, 13)
      assert(bitArray.ascending == Seq(13, 32, 45, 66, 77, 111))
      assert(bitArray.floor == 13)
    }

    it("should support remove(value)") {
      val bitArray = BitArray(17, 61, 95, 122)
      assert(bitArray.ascending == Seq(17, 61, 95, 122))

      bitArray.remove(95)
      assert(bitArray.ascending == Seq(17, 61, 122))

      bitArray.remove(122)
      assert(bitArray.ascending == Seq(17, 61))

      bitArray.remove(17)
      assert(bitArray.ascending == Seq(61))

      bitArray.remove(61)
      assert(bitArray.ascending.isEmpty)
    }

    it("should support removeAll(values: _*)") {
      val bitArray = BitArray(17, 61, 95, 122)
      assert(bitArray.ascending == Seq(17, 61, 95, 122))

      bitArray.removeAll()
      assert(bitArray.ascending.isEmpty)
    }

    it("should support takeAll()") {
      val bitArray = BitArray(17, 61, 95, 122)
      assert(bitArray.ascending == Seq(17, 61, 95, 122))

      val values = bitArray.takeAll()
      assert(values == List(17, 61, 95, 122))
      assert(bitArray.ascending.isEmpty)
    }

    it("should provide a rational toString") {
      val bitArray = BitArray.withRange(17, 123)
      bitArray.add(17, 61, 95, 122)
      assert(bitArray.toString == "BitArray(17, 61, 95, 122)")
    }

    it("should encode/decode to/from a ByteBuffer") {
      val bitArray = BitArray.withRange(517, 823)
      bitArray.add(517, 661, 795, 822)

      val buf = bitArray.encode
      assert(buf.limit() == 52)
      assert(buf.capacity() == buf.limit())

      val decodedBitMap = BitArray.decode(buf)
      assert(decodedBitMap.ascending == Seq(517, 661, 795, 822))
    }

    it("should support visualization of the collection") {
      val bitArray = BitArray(0, 17, 32, 61, 64, 95, 107, 122, 128)
      assert(bitArray.size == 9)

      bitArray.dump.zipWithIndex foreach { case (line, n) => info(s"[${n + 1}] $line") }
      assert(bitArray.dump == Seq(
        "..1............................1..............1................1",
        ".....1..............1...........1..............................1",
        "...............................................................1"
      ))
    }

    it("should support all features with min and max values defined") {
      val bitArray = BitArray.withRange(minValue = 10000, maxValue = 10064)
      val offsets = (10000L until 10064L).toList

      // add & contains
      bitArray.add(offsets: _*)
      offsets.map(bitArray.contains).forall(_ == true)

      // encode & decode & ascending
      val buf = bitArray.encode
      assert(buf.limit() == 20)
      assert(buf.capacity() == buf.limit())
      assert(BitArray.decode(buf).ascending == offsets)

      // count
      assert(bitArray.count(_ > 10032) == 31)

      // descending
      assert(bitArray.descending == offsets.reverse)

      // foldLeft
      assert(bitArray.foldLeft(0L) { (sum, value) => sum + value } == 642016)

      // map
      assert(bitArray.map(_ - 10000).sum == 2016)

      // remove (last) & ascending
      bitArray.remove(offsets.last)
      assert(bitArray.ascending == offsets.init)

      // remove (first) & ascending
      bitArray.remove(offsets.head)
      assert(bitArray.ascending == offsets.init.tail)

      // size
      assert(bitArray.size == 62)

      // remove (middle) & dump
      bitArray.remove(10031)
      assert(bitArray.dump == Seq(".1111111111111111111111111111111.111111111111111111111111111111."))
    }

    it("should notify observers of mutations made to the bit array") {
      val bitArray = BitArray(1, 2, 3, 5, 7, 11, 13)
      var changes: Int = 0
      bitArray.addChangeListener((_: BitArray) => changes += 1)

      // removing 1 should trigger a change event
      bitArray.remove(1)
      assert(changes == 1)

      // since 1 has already been removed it should not trigger a change event
      bitArray.remove(1)
      assert(changes == 1)

      // adding 17 should trigger a change event
      bitArray.add(17)
      assert(changes == 2)

      // adding 17 again should not trigger a change event
      bitArray.add(17)
      assert(changes == 2)

      // removing 13 should trigger a change event
      bitArray.remove(13)
      assert(changes == 3)

      // removing 13 again should not trigger a change event
      bitArray.remove(13)
      assert(changes == 3)
    }

  }

}
