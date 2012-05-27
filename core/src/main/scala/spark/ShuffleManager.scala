package spark

import java.io._
import java.util.concurrent.atomic.AtomicLong

class ShuffleManager extends Logging {
  private val nextShuffleId = new AtomicLong(0)

  private var shuffleDir: File = null
  private var serverUri: String = null
  private var server: Option[HttpServer] = None

  initialize()

  private def initialize() {
    val localDir = createTemporaryDirectory()

    shuffleDir = new File(localDir, "shuffle")
    shuffleDir.mkdirs()
    logInfo("Shuffle dir: " + shuffleDir)

    val extServerPort = System.getProperty("spark.localFileShuffle.external.server.port", "-1").toInt
    if (extServerPort != -1) {
      // We're using an external HTTP server; set URI relative to its root
      var extServerPath = System.getProperty("spark.localFileShuffle.external.server.path", "")
      if (extServerPath != "" && !extServerPath.endsWith("/")) {
        extServerPath += "/"
      }
      serverUri = "http://%s:%d/%s/%s".format(Utils.localIpAddress(), extServerPort, extServerPath, localDir.getName)
    } else {
      // Create our own server
      server = Some(new HttpServer(localDir))
      server.get.start()
      serverUri = server.get.uri
    }
    logInfo("Local URI: " + serverUri)
  }

  def stop() {
    server.map(_.stop)
  }

  def getOutputFile(shuffleId: Long, inputId: Int, outputId: Int): File = {
    val dir = new File(shuffleDir, shuffleId + "/" + inputId)
    dir.mkdirs()
    new File(dir, "" + outputId)
  }

  def getServerUri: String = serverUri

  def newShuffleId(): Long = nextShuffleId.getAndIncrement()

  /**
   * Create temporary directory
   */
  private def createTemporaryDirectory(): File = {
    val localDirRoot = System.getProperty("spark.local.dir", "/tmp")

    try {
      //create temporary directory
      val localDir = Utils.createTempDir(localDirRoot, "spark-local-")

      // Add a shutdown hook to delete the local dir
      Runtime.getRuntime.addShutdownHook(new Thread("delete Spark local dir") {
        override def run() {
          Utils.deleteRecursively(localDir)
        }
      })

      localDir
    } catch {
      case _ =>
        logError("Failed 10 attempts to create local dir in " + localDirRoot)
        sys.exit(1)
    }
  }
}
