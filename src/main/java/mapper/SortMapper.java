package mapper;

import model.FinalResultWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

import static common.Constants.DEFAULT_HADOOP_SEPARATOR;

public class SortMapper extends Mapper<LongWritable, Text, DoubleWritable, FinalResultWritable> {

    private final DoubleWritable revenueKey = new DoubleWritable();
    private final FinalResultWritable resultValue = new FinalResultWritable();

    @Override
    protected void map(
            LongWritable key,
            Text value,
            Context context
    ) throws IOException, InterruptedException {

        String[] parts = value.toString().split(DEFAULT_HADOOP_SEPARATOR);
        if (parts.length != 3) {
            return;
        }

        String category = parts[0];
        double revenue = Double.parseDouble(parts[1]);
        long quantity = Long.parseLong(parts[2]);

        revenueKey.set(revenue);
        resultValue.setRevenue(revenue);
        resultValue.setQuantity(quantity);
        resultValue.setCategory(category);

        context.write(revenueKey, resultValue);
    }
}
