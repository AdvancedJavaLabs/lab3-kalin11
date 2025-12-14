package reducer;

import model.RevenueQuantityWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class SalesReducer extends Reducer<Text, RevenueQuantityWritable, Text, RevenueQuantityWritable> {

    private final RevenueQuantityWritable revenueQuantityWritable = new RevenueQuantityWritable();

    @Override
    protected void reduce(
            Text key,
            Iterable<RevenueQuantityWritable> values,
            Reducer<Text, RevenueQuantityWritable, Text, RevenueQuantityWritable>.Context context
    ) throws IOException, InterruptedException {
        double totalRevenue = 0.0;
        long totalQuantity = 0;

        for (RevenueQuantityWritable val : values) {
            totalRevenue += val.getRevenue();
            totalQuantity += val.getQuantity();
        }

        revenueQuantityWritable.setQuantity(totalQuantity);
        revenueQuantityWritable.setRevenue(totalRevenue);

        context.write(key, revenueQuantityWritable);
    }
}
