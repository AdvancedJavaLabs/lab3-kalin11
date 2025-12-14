package job;

import lombok.extern.slf4j.Slf4j;
import mapper.SalesMapper;
import model.RevenueQuantityWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import reducer.SalesReducer;

@Slf4j
public class SalesAggregationJob implements Tool {

    private Configuration configuration;

    @Override
    public int run(String[] args) throws Exception {
        if (args.length < 2) {
            log.error("Invalid args length");
            return 1;
        }

        String inputPath = args[0];
        String outputPath = args[1];
        int numberOfReducers = args.length > 2 ? Integer.parseInt(args[2]) : 1;

        log.info("Sales Aggregation started: input={}, output={}, reducers={}", inputPath, outputPath, numberOfReducers);

        Job job = Job.getInstance(configuration, "Sales Aggregation");
        job.setJarByClass(SalesAggregationJob.class);

        job.setMapperClass(SalesMapper.class);
        job.setReducerClass(SalesReducer.class);
        job.setNumReduceTasks(numberOfReducers);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(RevenueQuantityWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(RevenueQuantityWritable.class);

        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        boolean success = job.waitForCompletion(true);
        return success ? 0 : 1;
    }

    @Override
    public void setConf(Configuration conf) {
        this.configuration = conf;
    }

    @Override
    public Configuration getConf() {
        return configuration;
    }
}
