import job.SalesAggregationJob;
import job.SortJob;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;

@Slf4j
public class Main {

    @SneakyThrows
    public static void main(String[] args) {
        if (args.length < 2) {
            log.error("Invalid args length");
            return;
        }

        Args parsedArgs = parseArgs(args);

        Configuration configuration = new Configuration();
        configuration.set("mapreduce.input.fileinputformat.split.maxsize", String.valueOf(parsedArgs.blockSize * 1024L));

        String tempDirectory = parsedArgs.output + "_temp";

        long start = System.currentTimeMillis();

        log.info("Aggregation phase started");
        String[] salesAggregationArguments = {parsedArgs.input, tempDirectory, String.valueOf(parsedArgs.reducers)};
        int salesAggregationResult = ToolRunner.run(configuration, new SalesAggregationJob(), salesAggregationArguments);

        if (salesAggregationResult != 0) {
            log.error("Aggregation phase threw an error");
            return;
        }

        log.info("Sorting phase started");
        String[] sortArguments = {tempDirectory, parsedArgs.output, String.valueOf(parsedArgs.reducers)};
        int sortResult = ToolRunner.run(configuration, new SortJob(), sortArguments);

        if (sortResult != 0) {
            log.error("Sorting phase threw an error");
            return;
        }

        long end = System.currentTimeMillis();
        long duration = end - start;

        log.info("Execution completed");
        log.info("Total time: {} ms ({} sec)", duration, duration / 1000.0);
    }

    private static Args parseArgs(
            String[] args
    ) {
        String inputDir = args[0];
        String outputDir = args[1];

        int reducers = 2;
        // in kB
        int blockSize = 128;

        if (args.length >= 3) {
            reducers = Integer.parseInt(args[2]);
        }
        if (args.length >= 4) {
            blockSize = Integer.parseInt(args[3]);
        }

        return new Args(
                inputDir, outputDir, reducers, blockSize
        );
    }

    @Data
    @AllArgsConstructor
    private static class Args {
        private String input;
        private String output;
        private Integer reducers;
        private Integer blockSize;
    }
}
