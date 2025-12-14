package reducer;

import lombok.extern.slf4j.Slf4j;
import model.FinalResultWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

@Slf4j
public class SortReducer extends Reducer<DoubleWritable, FinalResultWritable, Text, Text> {

    @Override
    protected void reduce(
            DoubleWritable key,
            Iterable<FinalResultWritable> values,
            Context context
    ) throws IOException, InterruptedException {

        for (FinalResultWritable value : values) {
            double revenue = key.get();
            String outputValue = String.format("%.2f\t%d", revenue, value.getQuantity());
            context.write(new Text(value.getCategory()), new Text(outputValue));
        }
    }
}
