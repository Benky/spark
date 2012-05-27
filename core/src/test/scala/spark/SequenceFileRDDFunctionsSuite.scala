package spark

import org.scalatest.FunSuite
import SparkContext._
import org.apache.hadoop.io._

class SequenceFileRDDFunctionsSuite extends FunSuite {

  test("getWritableClass") {
    assert(SequenceFileRDDFunctions.getWritableClass[Int] === classOf[IntWritable])
    assert(SequenceFileRDDFunctions.getWritableClass[Long] === classOf[LongWritable])
    assert(SequenceFileRDDFunctions.getWritableClass[Float] === classOf[FloatWritable])
    assert(SequenceFileRDDFunctions.getWritableClass[Double] === classOf[DoubleWritable])
    assert(SequenceFileRDDFunctions.getWritableClass[Boolean] === classOf[BooleanWritable])
    assert(SequenceFileRDDFunctions.getWritableClass[String] === classOf[Text])
    assert(SequenceFileRDDFunctions.getWritableClass[Array[Byte]] === classOf[BytesWritable])

    assert(SequenceFileRDDFunctions.getWritableClass[NullWritable] === classOf[NullWritable])
    assert(SequenceFileRDDFunctions.getWritableClass[IntWritable] === classOf[IntWritable])
    assert(SequenceFileRDDFunctions.getWritableClass[LongWritable] === classOf[LongWritable])
    assert(SequenceFileRDDFunctions.getWritableClass[FloatWritable] === classOf[FloatWritable])
    assert(SequenceFileRDDFunctions.getWritableClass[DoubleWritable] === classOf[DoubleWritable])
    assert(SequenceFileRDDFunctions.getWritableClass[BooleanWritable] === classOf[BooleanWritable])
    assert(SequenceFileRDDFunctions.getWritableClass[Text] === classOf[Text])
    assert(SequenceFileRDDFunctions.getWritableClass[BytesWritable] === classOf[BytesWritable])
  }
}
