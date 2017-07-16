package managers;

import assets.DoubleArrayWritable;

import java.io.IOException;
// import java.io.BufferedReader;
// import java.io.StringReader;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class JobManager {
    public static class TokenizerMapper extends Mapper<Object, Text, IntWritable, DoubleArrayWritable>{
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // BufferedReader buff = new BufferedReader(new StringReader(value.toString()));
            
            IntWritable rank = new IntWritable();
            Random r = new Random();
            rank.set(r.nextInt());

            // String[] tokens;
            // String line;
            double[] values = new double[5];
            for (int i = 0; i < 5; i++) {
                values[i] = r.nextDouble();
            }

            DoubleArrayWritable output = new DoubleArrayWritable(values);
            context.write(rank, output);
        }
    }

    public static class IntSumReducer extends Reducer<IntWritable,DoubleArrayWritable,IntWritable,DoubleWritable> {
        public void reduce(IntWritable key, DoubleArrayWritable values, Context context) throws IOException, InterruptedException {
            // int sum = 0;
            // for (IntWritable val : values) {
            //   sum += val.get();
            // }
            // result.set(sum);
            Random r = new Random();
            key.set(r.nextInt());
            double[] array = values.get();
            for (double val: array) {
                DoubleWritable output = new DoubleWritable(val);
                context.write(key, output);
            }
        }
    }

    public void run(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "word count");
        job.setJarByClass(JobManager.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(DoubleArrayWritable.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(DoubleWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path("output"));
        job.waitForCompletion(true);
    }
}