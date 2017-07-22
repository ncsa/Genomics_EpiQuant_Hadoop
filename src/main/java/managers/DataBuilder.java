package managers;

import utilities.ConfSet;

import java.io.IOException;
import java.io.BufferedReader;
import java.io.StringReader;
import java.util.Random;
import java.util.Set;
import java.util.HashSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.chain.ChainReducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.commons.math3.util.FastMath;
import org.apache.commons.math3.distribution.TDistribution;
import org.apache.commons.math3.stat.regression.OLSMultipleLinearRegression;

public class DataBuilder {
    public static class TokenMapper extends Mapper<Object, Text, Text, Text>{
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            BufferedReader buff = new BufferedReader(new StringReader(value.toString()));
            String line;
            String[] tokens;
            Configuration conf = context.getConfiguration();
            String mapKey = ConfSet.getY(conf);

            while ((line = buff.readLine()) != null) {
            
            }
        }
    }

    public static class ElementReducer extends Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        
        }
    }

    public Job run() throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "word count");
        job.setJarByClass(DataBuilder.class);
        job.setMapperClass(TokenMapper.class);
        job.setCombinerClass(ElementReducer.class);
        job.setReducerClass(ElementReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        FileInputFormat.addInputPath(job, new Path(""));
        FileOutputFormat.setOutputPath(job, new Path("DataBuilder"));

        job.waitForCompletion(true);
        return job;
    }
}