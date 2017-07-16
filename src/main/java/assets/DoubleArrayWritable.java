package assets;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

public class DoubleArrayWritable implements Writable {
    private IntWritable length;
    private DoubleWritable[] array;

    public DoubleArrayWritable() {
        
    }

    public DoubleArrayWritable(DoubleWritable[] input) {
        this.array = new DoubleWritable[input.length];
        for (int i = 0; i < input.length; i++) {
            this.array[i] = new DoubleWritable(input[i].get());
        }
        this.length = new IntWritable(this.array.length);
    }

    public DoubleWritable[] get() {
        DoubleWritable[] out = new DoubleWritable[this.array.length];
        for (int i = 0; i < out.length; i++) {
            out[i] = new DoubleWritable(this.array[i].get());
        }
        return out;
    }

    public void set(DoubleWritable[] input) {
        this.array = new DoubleWritable[input.length];
        for (int i = 0; i < input.length; i++) {
            this.array[i] = new DoubleWritable(input[i].get());
        }
        this.length = new IntWritable(this.array.length);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        this.length.write(out);
        for (int i = 0; i < this.array.length; i++) {
            this.array[i].write(out);
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.length.readFields(in);
        this.array = new DoubleWritable[this.length.get()];
        for (int i = 0; i < this.array.length; i++) {
            DoubleWritable getVar = new DoubleWritable();
            getVar.readFields(in);
            this.array[i] = new DoubleWritable(getVar.get());
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