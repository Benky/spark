package spark

class SparkException(message: String, reason: Throwable) extends Exception(message, reason) {
  def this(message: String) = this(message, null)
}
