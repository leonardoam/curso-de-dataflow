import com.spotify.scio.ContextAndArgs
import org.slf4j.LoggerFactory

object ExercicioLegal {
  val logger = LoggerFactory.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {
    val (sc, _) = ContextAndArgs(args)

    sc.withName("read file").textFile("gs://arquivei-curso-dataflow/big.txt")
        .withName("picota as letra tudo").flatMap { line =>
          for (c <- line) yield (c, 1)
        }
        .groupByKey
        .map { case(c, v) =>
          logger.info(s"v: $v sum: ${v.sum}")
          (c, v.sum)
        }
        .saveAsTextFile("gs://arquivei-curso-dataflow/barbie/")

    sc.close().waitUntilFinish()
  }
}
