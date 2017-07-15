package managers;

import java.io.IOException;
import java.io.BufferedReader;
import java.io.StringReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class JobManager {
    public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable>{
        private IntWritable rank = new IntWritable();
        private Text word = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            BufferedReader buff = new BufferedReader(new StringReader(value.toString()));
            
            String[] tokens;
            String line;
            while ((line = buff.readLine()) != null) {
                System.out.println(line);
                tokens = line.split("\\t");
                for (int i = 0; i < tokens.length; i++) {
                    System.out.println(tokens[i]);
                }
            }
            System.exit(0);
            //   StringTokenizer itr = new StringTokenizer(value.toString());
            //   while (itr.hasMoreTokens()) {
            //     word.set(itr.nextToken());
            //     context.write(word, one);
            //   }
            // }
        }
    }

    public static class IntSumReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
              sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public void run(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "word count");
        job.setJarByClass(JobManager.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path("output"));
        job.waitForCompletion(true);
    }
}