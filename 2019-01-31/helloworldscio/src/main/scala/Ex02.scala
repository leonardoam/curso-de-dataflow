import com.spotify.scio.ContextAndArgs
import org.slf4j.LoggerFactory

object Ex02 {
  val logger = LoggerFactory.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {
    val (sc, _) = ContextAndArgs(args)

    sc.pubsubSubscription[String]("projects/arquivei-curso-dataflow/subscriptions/curso-dataflow")
      .flatMap { message =>
        Thread.sleep(1000)
        logger.info(s"random=${Math.random()}")
        Seq(message, message)
      }
      .saveAsPubsub("projects/arquivei-curso-dataflow/topics/curso-dataflow")

    sc.close().waitUntilFinish()
  }
}
