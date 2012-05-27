package spark

import org.scalatest.FunSuite
import util.Random
import collection.mutable.HashMap

class RDDSuite extends FunSuite {
  test("basic operations") {
    val sc = new SparkContext("local", "test")
    val nums = sc.makeRDD(Array(1, 2, 3, 4), 2)
    assert(nums.collect().toList === List(1, 2, 3, 4))
    assert(nums.toArray().toList === nums.collect().toList)
    assert(nums.reduce(_ + _) === 10)
    assert(nums.fold(0)(_ + _) === 10)
    assert(nums.map(_.toString).collect().toList === List("1", "2", "3", "4"))
    assert(nums.filter(_ > 2).collect().toList === List(3, 4))
    assert(nums.flatMap(x => 1 to x).collect().toList === List(1, 1, 2, 1, 2, 3, 1, 2, 3, 4))
    assert(nums.union(nums).collect().toList === List(1, 2, 3, 4, 1, 2, 3, 4))
    assert((nums ++ nums).collect().toList === List(1, 2, 3, 4, 1, 2, 3, 4))
    assert(nums.glom().map(_.toList).collect().toList === List(List(1, 2), List(3, 4)))
    val partitionSums = nums.mapPartitions(iter => Iterator(iter.reduceLeft(_ + _)))
    assert(partitionSums.collect().toList === List(3, 7))
    sc.stop()
  }

  test("count") {
    val sc = new SparkContext("local", "test")

    assert(sc.makeRDD(Nil, 2).count() === 0)

    assert(sc.makeRDD(Array(1, 2, 3, 4), 2).count() === 4)

    sc.stop()
  }

  test("first") {
    val sc = new SparkContext("local", "test")

    //head operation on empty collection isn't supported
    intercept[UnsupportedOperationException] {
      sc.makeRDD(Nil, 2).first()
    }

    assert(sc.makeRDD(Array(1, 2, 3, 4), 2).first() === 1)

    sc.stop()
  }

  test("groupBy") {
    val sc = new SparkContext("local", "test")

    val strings = Seq.fill(50 + Random.nextInt(50))(Random.nextString(1 + Random.nextInt(10)))
    val strs = sc.makeRDD(strings)
    assert(strs.groupBy(_.length).collect().toMap === strings.groupBy(_.length))

    sc.stop()
  }

  test("take") {
    val sc = new SparkContext("local", "test")

    val nums = sc.makeRDD(Array(1, 2, 3))
    val numsEmpty = sc.makeRDD(Nil)

    assert(numsEmpty.take(0) === Array())
    assert(numsEmpty.take(-1) === Array())
    assert(nums.take(-1) === Array())
    assert(nums.take(1) === Array(1))
    assert(nums.take(3) === Array(1, 2, 3))
    assert(nums.take(5) === Array(1, 2, 3))

    sc.stop()
  }

  test("reduce") {
    val sc = new SparkContext("local", "test")

    val nums = sc.makeRDD(1 to 4)
    assert(nums.reduce(_ + _) === 10)

    intercept[UnsupportedOperationException] {
      assert(sc.makeRDD(List[Int]()).reduce(_ + _) === 0)
    }

    assert(sc.makeRDD(List("1", "2", "$", "4")).reduce(_ + _) === "12$4")

    sc.stop()
  }

  test("fold") {
    val sc = new SparkContext("local", "test")

    val nums = sc.makeRDD(1 to 4)
    assert(nums.fold(0)(_ + _) === 10)

    assert(sc.makeRDD(List[Int]()).fold(0)(_ + _) === 0)

    assert(sc.makeRDD(List("1", "2", "$", "4")).fold("")(_ + _) === "12$4")

    sc.stop()
  }

  test("takeSample without replacement") {
    val sc = new SparkContext("local", "test")

    val nums = (1 to 100).toArray
    val nums50 = sc.makeRDD(nums).takeSample(false, 50, 20)
    val nums101 = sc.makeRDD(nums).takeSample(false, 101, 20)
    assert(nums50.size === 50)
    assert(nums50.distinct.diff(nums) === Array())

    assert(nums101.size === 100)
    assert(nums50.distinct.diff(nums) === Array())

    intercept[IllegalArgumentException] {
      sc.makeRDD(Nil).takeSample(false, -1, 20)
    }
    sc.stop()
  }

  test("sample without replacement") {
    val sc = new SparkContext("local", "test")

    val nums = (1 to 100).toArray
    assert(sc.makeRDD(nums).sample(false, 1, 20).collect() === nums)
    assert(sc.makeRDD(nums).sample(false, 0, 20).collect().size < 100)

    sc.stop()
  }

  test("sample with replacement") {
    val sc = new SparkContext("local", "test")

    val nums = (1 to 100).toArray
    assert(sc.makeRDD(nums).sample(true, 1, 20).collect().distinct.diff(nums) === Array())
    assert(sc.makeRDD(nums).sample(true, 0, 20).collect().size < 100)

    sc.stop()
  }

  test("aggregate") {
    val sc = new SparkContext("local", "test")
    val pairs = sc.makeRDD(Array(("a", 1), ("b", 2), ("a", 2), ("c", 5), ("a", 3)))
    type StringMap = HashMap[String, Int]
    val emptyMap = new StringMap {
      override def default(key: String): Int = 0
    }
    val mergeElement: (StringMap, (String, Int)) => StringMap = (map, pair) => {
      map(pair._1) += pair._2
      map
    }
    val mergeMaps: (StringMap, StringMap) => StringMap = (map1, map2) => {
      for ((key, value) <- map2) {
        map1(key) += value
      }
      map1
    }
    val result = pairs.aggregate(emptyMap)(mergeElement, mergeMaps)
    assert(result.toSet === Set(("a", 6), ("b", 2), ("c", 5)))
    sc.stop()
  }
}
