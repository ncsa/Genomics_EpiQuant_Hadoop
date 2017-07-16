package assets;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class DoubleArrayWritable implements Writable {
    private int length;
    private double[] array;

    public DoubleArrayWritable() {
        
    }

    public DoubleArrayWritable(double[] input) {
        this.length = input.length;
        this.array = input;
    }

    public double[] get() {
        return this.array;
    }

    public void set(double[] input) {
        this.length = input.length;
        this.array = input;
    }

    public void write(DataOutput out) throws IOException {
        out.writeInt(this.length);
        for (double value: this.array) {
            out.writeDouble(value);
        }
    }

    public void readFields(DataInput in) throws IOException {
        this.length = in.readInt();
        array = new double[length];
        for (int i = 0; i < length; i++) {
            array[i] = in.readDouble();
        }
    }

    // @Override
    // public void write(DataOutput out) throws IOException {
    //     out.writeInt(data.length);
    //     for (DoubleWritable point: data) {
    //         double value = point.get();
    //         out.writeDouble(value);
    //     }
    // }

    // @Override
    // public void readFields(DataInput in) throws IOException {
    //     int length = in.readInt();
    //     DoubleWritable[] dataTemp = new DoubleWritable[length];
    //     for (int i = 0; i < length; i++) {
    //         dataTemp[i] = new DoubleWritable(in.readDouble());
    //     }
    //     this.data = dataTemp;
    // }
}