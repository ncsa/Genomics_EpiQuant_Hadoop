package managers;

import utilities.ConfSet;

import java.io.IOException;
import java.io.BufferedReader;
import java.io.StringReader;
import java.util.Set;
import java.util.HashSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.commons.math3.stat.regression.SimpleRegression;
import org.apache.commons.math.stat.regression.MultipleLinearRegression;

public class JobManager {
    public static class LinRegMapper extends Mapper<Object, Text, Text, Text>{
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            BufferedReader buff = new BufferedReader(new StringReader(value.toString()));
            String line;
            String[] tokens;

            Configuration conf = context.getConfiguration();
            String mapKey = conf.get("y");
            double[] y = ConfSet.getY(mapKey);

			while ((line = buff.readLine()) != null) {
                tokens = line.split("\\t");
                String xNewLabel = tokens[0];
                // double[] xNew = new double[tokens.length - 1];
                // for (int i = 1; i < tokens.length; i++) {
                //     xNew[i - 1] = Double.parseDouble(tokens[i]);
                // }

                // for (int i = 0 ; i < xNew.length; i++) {
                //     xNewLabel += "\t" + xNew[i];
                // }
                context.write(new Text(mapKey), new Text(xNewLabel));
            }
            buff.close();
        }
    }

    public static class MaxSigReducer extends Reducer<Text, Text, Text, Text> {
        // private IntWritable result = new IntWritable();
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            // int max = 0;
            // Need iterable so that each key is only processed once.
            // for (IntWritable val: values) {
            //     max = Math.max(max, val.get());
            // }
            // result.set(max);
            for (Text val: values) {
                context.write(key, val);
            }
        }
    }

    public Job run(String[] args, String y, int phenotype, int split) throws Exception {
        Configuration conf = new Configuration();
        conf.set("y", y);
        Job job = Job.getInstance(conf, "job manager");
        job.setJarByClass(JobManager.class);
        
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(LinRegMapper.class);
        job.setCombinerClass(MaxSigReducer.class);
        job.setReducerClass(MaxSigReducer.class);

        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path("Phenotype-" + phenotype + ".Split-" + split));
        
        job.submit();
        return job;
    }
}