package managers;

import java.io.IOException;
import java.io.BufferedReader;
import java.io.StringReader;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.apache.commons.math3.stat.regression.SimpleRegression;

public class JobManager {
    public static class JobSplitMapper extends Mapper<Object, Text, Text, Text>{
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            BufferedReader buff = new BufferedReader(new StringReader(value.toString()));
            Random r = new Random();
            String[] tokens;
            String line;

			while ((line = buff.readLine()) != null) {
                try {
                    tokens = line.split("\\t");
                    String out = tokens[0];
                    for (int i = 1; i < 5; i++) {
                        out += "\t" + tokens[i];
                    }
                    context.write(new Text("" + r.nextInt(4)), new Text(out));
                } catch (Exception e) {
                    System.err.println("Could not parse a line");
                }
			}
        }
    }

    public static class LinRegReducer extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Text values, Context context) throws IOException, InterruptedException {
            context.write(key, values);
        }
    }

    public void run(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "job manager");
        job.setJarByClass(JobManager.class);
        
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(JobSplitMapper.class);
        job.setCombinerClass(LinRegReducer.class);
        job.setReducerClass(LinRegReducer.class);

        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path("output"));
        
        job.waitForCompletion(true);
    }
}