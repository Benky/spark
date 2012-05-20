package spark.cache

import org.scalatest.FunSuite
import spark.SparkEnv

class DiskSpillingCacheTest extends FunSuite {
  test("constructor test") {
    val cache = new DiskSpillingCache(40)
    expect(40)(cache.getCapacity)
  }

  test("caching") {
    SparkEnv.set(new SparkEnv(null,new spark.JavaSerializer(), null, null,null,null,null))

    val cache = new DiskSpillingCache(18)

    //should be OK
    expect(CachePutSuccess(10))(cache.put("1", 0, "Meh"))

    //we cannot add this to cache (there is not enough space in cache) & we cannot evict the only value from
    //cache because it's from the same dataset
    expect(CachePutFailure())(cache.put("1", 1, "Meh"))

    //should be OK, dataset '1' can be evicted from cache
    expect(CachePutSuccess(10))(cache.put("2", 0, "Meh"))

    //should fail, cache should obey it's capacity
    expect(CachePutFailure())(cache.put("3", 0, "Very_long_and_useless_string"))
  }
}
