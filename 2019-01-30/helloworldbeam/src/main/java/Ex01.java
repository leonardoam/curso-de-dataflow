import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

public class Ex01 {
    private static final Logger LOG = LoggerFactory.getLogger(Ex01.class);

    public static class MyFunction extends DoFn<Integer, String> {
        @ProcessElement
        public void processElement(ProcessContext context) {
            Integer value = context.element();

            LOG.info(String.format("value=%d", value));

            context.output(value.toString());
        }
    }

    public static void main(String args[]) {
        PipelineOptions options = PipelineOptionsFactory
                .fromArgs(args)
                .withValidation()
                .as(PipelineOptions.class);

        Pipeline p = Pipeline
                .create(options);

        p
                .apply(Create.of(Arrays.asList(1,2,3,4,5,6,7,8,9,10)))
                .apply(ParDo.of(new MyFunction()))
                .apply(TextIO
                            .write()
                            .to("gs://arquivei-curso-dataflow/ex01/"));

        p.run().waitUntilFinish();
    }
}

