import com.spotify.scio.ContextAndArgs
import com.spotify.scio.values.WindowOptions
import org.apache.beam.sdk.transforms.windowing.AfterPane
import org.apache.beam.sdk.values.WindowingStrategy.AccumulationMode
import org.slf4j.LoggerFactory

object Ex03 {
  val logger = LoggerFactory.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {
    val (sc, _) = ContextAndArgs(args)

    val maxElementInput = sc.pubsubSubscription[String]("projects/arquivei-curso-dataflow/subscriptions/side-value")
        .map { message =>
          message.toDouble
        }
        .withGlobalWindow(
          WindowOptions(
            trigger = AfterPane.elementCountAtLeast(1),
            accumulationMode = AccumulationMode.DISCARDING_FIRED_PANES
          )
        )
        .reduce { case(value1, value2) =>
          Math.max(value1, value2)
        }
        .asSingletonSideInput

    sc.pubsubSubscription[String]("projects/arquivei-curso-dataflow/subscriptions/value")
        .map { message =>
          message.toDouble
        }
        .withSideInputs(maxElementInput)
        .map { case(value, side) =>
          val maxValue = side(maxElementInput)
          logger.info(s"value=$value diff=${maxValue-value}")
        }

    sc.close().waitUntilFinish()
  }
}
