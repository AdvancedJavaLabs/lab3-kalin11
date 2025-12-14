package mapper;

import lombok.extern.slf4j.Slf4j;
import model.RevenueQuantityWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

@Slf4j
public class SalesMapper extends Mapper<LongWritable, Text, Text, RevenueQuantityWritable> {
    private static final String CSV_HEADER = "transaction_id,product_id,category,price,quantity";

    private final Text categoryKey = new Text();
    private final RevenueQuantityWritable revenueQuantityWritable = new RevenueQuantityWritable();

    @Override
    protected void map(
            LongWritable key,
            Text value,
            Mapper<LongWritable, Text, Text, RevenueQuantityWritable>.Context context
    ) throws IOException, InterruptedException {
        String line = value.toString();

        if (line.startsWith(CSV_HEADER)) {
            return;
        }

        String[] fields = line.split(",");
        if (fields.length != 5) {
            throw new IllegalArgumentException("Invalid params in csv file");
        }

        String category = fields[2];
        double price = Double.parseDouble(fields[3]);
        long quantity = Long.parseLong(fields[4]);

        double revenue = price * quantity;

        categoryKey.set(category);
        revenueQuantityWritable.setRevenue(revenue);
        revenueQuantityWritable.setQuantity(quantity);
        context.write(categoryKey, revenueQuantityWritable);
    }
}
