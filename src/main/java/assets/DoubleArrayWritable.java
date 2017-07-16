package assets;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.ArrayWritable;

public class DoubleArrayWritable extends ArrayWritable {
    public DoubleArrayWritable() {
        super(DoubleWritable.class);
    }

    public DoubleArrayWritable(double[] doubles) {
        super(DoubleWritable.class);
        DoubleWritable[] doubleWritables = new DoubleWritable[doubles.length];
        for (int i = 0; i < doubles.length; i++) {
            doubleWritables[i] = new DoubleWritable(doubles[i]);
        }
        set(doubleWritables);
    }
}