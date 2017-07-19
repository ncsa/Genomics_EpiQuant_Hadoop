package managers;

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
    public static class LinRegMapper extends Mapper<Object, Text, Text, IntWritable>{
        Text mapKey = new Text();
        IntWritable mapValue = new IntWritable();
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // TODO: New conf.get to bring in x values
            // TODO: Build set of included x values
            // TODO: Fix data set to include headers.
            // TODO: Convert lines to double[]
            // TODO: Perform linear or multi linear regression
            // TODO: Write out signficance and x.

            // BufferedReader buff = new BufferedReader(new StringReader(value.toString()));
            // String line;
            // String[] tokens;
            // boolean first = true;

			// while ((line = buff.readLine()) != null) {
            //     if (first) {
            //         tokens = line.split("\\t");
            //     } else {
            //         first = false;
            //     }
            // }

            Configuration conf = context.getConfiguration();
            String yString = conf.get("y"); // key for set of y values.
            // double[] y = getY(yString); // y values we're comparing against.
            // String[][] xStrings = getX(conf);
            // double[][] xModel = convertX(xStrings); // x values already in the model.
            // Set<String> xSet = storeX(xStrings); // x names already in the model.

            mapKey.set(yString);
            mapValue.set(1);
            context.write(mapKey, mapValue);
        }

        // Gets the y value from the context configuration.
        public double[] getY(String yString) {
            String[] yStrings = yString.split("\\t");
            double[] y = new double[yStrings.length - 1];
            for (int i = 1; i < yStrings.length; i++) {
                y[i - 1] = Double.parseDouble(yStrings[i]);
            }
            return y;
        }

        // Gets the x values from the context configuration.
        public String[][] getX(Configuration conf) {
            String[] xs = conf.get("x").split(":");
            String[][] xsArray = new String[xs.length][];
            for (int i = 0; i < xs.length; i++) {
                xsArray[i] = xs[i].split(",");
            }
            return xsArray;
        }

        // Convert the x values from strings to doubles.
        public double[][] convertX(String[][] xStrings) {
            double[][] xValues = new double[xStrings.length - 1][xStrings[0].length - 1];
            for (int i = 1; i < xStrings.length; i++) {
                for (int j = 1; j < xStrings[0].length; i++) {
                    xValues[i - 1][j - 1] = Double.parseDouble(xStrings[i][j]);
                }
            }
            return xValues;
        }

        // Gets the x value labels from the context configuration.
        public Set<String> storeX(String[][] xStrings) {
            Set<String> xSet = new HashSet<String>();
            for (int i = 0; i < xStrings.length; i++) {
                xSet.add(xStrings[i][0]);
            }
            return xSet;
        }
    }

    public static class MaxSigReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val: values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public Job run(String[] args, String y, int phenotype, int split) throws Exception {
        Configuration conf = new Configuration();
        conf.set("y", y);
        Job job = Job.getInstance(conf, "job manager");
        job.setJarByClass(JobManager.class);
        
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setMapperClass(LinRegMapper.class);
        job.setCombinerClass(MaxSigReducer.class);
        job.setReducerClass(MaxSigReducer.class);

        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path("Phenotype-" + phenotype + ".Split-" + split));
        
        job.submit();
        return job;
    }
}