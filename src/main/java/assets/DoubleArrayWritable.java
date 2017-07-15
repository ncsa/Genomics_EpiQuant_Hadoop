package assets;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.DoubleWritable;

public class DoubleArrayWritable extends ArrayWritable {
    public DoubleWritable[] data;
    public DoubleArrayWritable() {
        super(DoubleWritable.class);
    }

    public DoubleArrayWritable(DoubleWritable[] input) {
        super(DoubleWritable.class);
        this.data = input;
    }

    public void set(DoubleWritable[] input) {
        this.data = input;
    }

    public DoubleWritable[] get() {
        return data;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(data.length);
        for (DoubleWritable point: data) {
            double value = point.get();
            out.writeDouble(value);
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        int length = in.readInt();
        DoubleWritable[] dataTemp = new DoubleWritable[length];
        for (int i = 0; i < length; i++) {
            dataTemp[i].set(in.readDouble());
        }
        this.data = dataTemp;
    }
}