package managers;

import assets.DoubleArrayWritable;

import java.io.IOException;
// import java.io.BufferedReader;
// import java.io.StringReader;
import java.util.Random;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class JobManager {
    public static class TokenizerMapper extends Mapper<Object, Text, IntWritable, ArrayWritable>{
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // BufferedReader buff = new BufferedReader(new StringReader(value.toString()));
            
            Random r = new Random();

            // String[] tokens;
            // String line;
            double[] values = new double[5];
            for (int i = 0; i < values.length; i++) {
                values[i] = r.nextDouble();
            }
            for (int i = 0; i < 3; i++) {
                context.write(new IntWritable(r.nextInt()), new DoubleArrayWritable(values));
            }
        }
    }

    public static class IntSumReducer extends Reducer<IntWritable, ArrayWritable, IntWritable, DoubleWritable> {
        public void reduce(IntWritable key, ArrayWritable values, Context context) throws IOException, InterruptedException {
            Writable[] writables = values.get();
            for (Writable writable: writables) {
                DoubleWritable doubleWritable = (DoubleWritable)writable;
                context.write(key, doubleWritable);
            }
        }
    }

    public void run(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "word count");
        job.setJarByClass(JobManager.class);
        
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(DoubleArrayWritable.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(DoubleWritable.class);

        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);

        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path("output"));
        
        job.waitForCompletion(true);
    }
}