import com.spotify.scio.ContextAndArgs
import com.spotify.scio.values.SCollection.State
import org.apache.beam.sdk.state.{StateSpecs, ValueState}
import org.apache.beam.sdk.transforms.{DoFn, ParDo}
import org.apache.beam.sdk.transforms.DoFn.{ProcessElement, StateId}
import org.apache.beam.sdk.values.KV
import org.slf4j.LoggerFactory

object Ex01 {
  val logger = LoggerFactory.getLogger(this.getClass)

  class SumPerKey extends DoFn[KV[String, Double], KV[String, Double]] {
    @StateId("keySum") private val keySum = StateSpecs.value[java.lang.Double]()

    @ProcessElement
    def processElement(ctx: ProcessContext,
                       @StateId("keySum") elemKeySum: ValueState[java.lang.Double]) = {
      val kv = ctx.element()

      val key = kv.getKey
      val value = kv.getValue

      val currentSum = elemKeySum.read()
      logger.info(s"kv=${kv.toString} currentSum=$currentSum")

      val newSum = currentSum+value
      logger.info(s"kv=${kv.toString} newSum=$newSum")

      elemKeySum.write(newSum)

      ctx.output(kv)
    }
  }

  def main(args: Array[String]): Unit = {
    val (sc, _) = ContextAndArgs(args)

    sc.pubsubSubscription[String]("projects/arquivei-curso-dataflow/subscriptions/curso-dataflow")
      .withName("print all args").map { value =>
        logger.info(s"value=$value")
        val tokens = value.split("=")
        (tokens(0), tokens(1).toDouble)
      }
      .withGlobalWindow()
      .withName("sum values").applyPerKeyDoFn(new SumPerKey)

    sc.close().waitUntilFinish()
  }
}
