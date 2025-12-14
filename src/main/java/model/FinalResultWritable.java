package model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class FinalResultWritable implements Writable {
    private String category;
    private double revenue;
    private long quantity;

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeDouble(revenue);
        out.writeLong(quantity);
        out.writeUTF(category);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        revenue = in.readDouble();
        quantity = in.readLong();
        category = in.readUTF();
    }

    @Override
    public String toString() {
        return String.format("%s\t%.2f\t%d", category, revenue, quantity);
    }
}
