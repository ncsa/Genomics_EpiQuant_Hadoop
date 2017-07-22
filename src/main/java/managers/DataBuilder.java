package managers;

import utilities.ConfSet;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.BufferedReader;
import java.io.StringReader;
import java.util.Random;
import java.util.Set;
import java.util.HashSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.IntWritable;
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
        private String pathString;
        private Path path;
        private FileSystem fs;

        @Override
        public void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            pathString = conf.get("path");
            path = new Path(pathString);
            fs = FileSystem.get(conf);
        }

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            BufferedReader fileBuff = new BufferedReader(new InputStreamReader(fs.open(path)));
            String fileLine;

            while ((fileLine = fileBuff.readLine()) != null) {
                String[] fileTokens = fileLine.split("\\t");
                String fileString = fileTokens[0];
                for (int i = 0; i < fileTokens.length; i++) {
                    fileString += "," + fileTokens[i];
                }
                BufferedReader valueBuff = new BufferedReader(new StringReader(value.toString()));
                String valueLine;
                while ((valueLine = valueBuff.readLine()) != null) {
                    if (!valueLine.equals(fileLine)) {
                        String[] valueTokens = valueLine.split("\\t");
                        String valueString = valueTokens[0];
                        for (int j = 0; j < valueTokens.length; j++) {
                            valueString += "," + valueTokens[j];
                        }
                        context.write(new Text(fileString), new Text(valueString));
                    }
                }
            }
        }
    }

    public static class ElementReducer extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Text values, Context context) throws IOException, InterruptedException {
            context.write(key, values);
        }
    }

    public Job run(String inputPath, String outputDir) throws Exception {
        Configuration conf = new Configuration();
        conf.set("path", inputPath);
        Job job = Job.getInstance(conf, "data builder");

        job.setJarByClass(DataBuilder.class);

        job.setMapperClass(TokenMapper.class);
        job.setCombinerClass(ElementReducer.class);
        job.setReducerClass(ElementReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputDir));

        job.waitForCompletion(true);
        return job;
    }
}