package common;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class DoubleDecreasingComparator extends WritableComparator {

    public DoubleDecreasingComparator() {
        super(DoubleWritable.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        DoubleWritable d1 = (DoubleWritable) a;
        DoubleWritable d2 = (DoubleWritable) b;

        return d2.compareTo(d1);
    }

    @Override
    public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
        double thisValue = readDouble(b1, s1);
        double thatValue = readDouble(b2, s2);

        return Double.compare(thatValue, thisValue);
    }
}
