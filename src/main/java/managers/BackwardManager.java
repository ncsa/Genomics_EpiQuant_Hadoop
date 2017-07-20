package managers;

import utilities.ConfSet;

import java.io.IOException;
import java.io.BufferedReader;
import java.io.StringReader;
import java.util.Set;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.commons.math3.stat.regression.SimpleRegression;
import org.apache.commons.io.output.NullOutputStream;
import org.apache.commons.math.stat.regression.GLSMultipleLinearRegression;
import org.apache.commons.math.stat.regression.MultipleLinearRegression;

public class BackwardManager {
    public static class LinRegMapper extends Mapper<Object, Text, Text, Text>{
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // BufferedReader buff = new BufferedReader(new StringReader(value.toString()));
            // String line;
            // String[] tokens;
            // Configuration conf = context.getConfiguration();
            // String mapKey = ConfSet.getY(conf);
            // ArrayList<double[]> xModel = new ArrayList<double[]>();

            // while ((line = buff.readLine()) != null) {
            //     tokens = line.split("\\t");
            //     SimpleRegression regression = new SimpleRegression();

            //     // Converts yString, and x tokens to double[].
            //     // Combines x and y data and adds them to regression object.
            //     regression.addData(ConfSet.combineData(ConfSet.convertXNew(tokens), ConfSet.convertY(mapKey)));
            //     try {
            //         double significance = regression.getSignificance();
            //         if (significance < 0.05) {
            //             context.write(new Text(mapKey), new Text(Double.toString(significance) + "\t" + ConfSet.getXNewString(tokens)));
            //         }
            //     } catch (Exception e) {
            //         System.err.println("Invalid significance generated.");
            //     }
            // }
            // buff.close();
        }
    }

    public static class MinSigReducer extends Reducer<Text, Text, Text, Text> {
        // private Text minX = new Text();

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            // double tempMinP = 0.05;
            // String tempMinX = "";
            // String[] tokens;

            // for (Text val: values) {
            //     tokens = val.toString().split("\\t");
            //     // If current is less do nothing.
            //     if (!(tempMinP < Double.parseDouble(tokens[0]))) { 
            //         // If current is greater, replace.
            //         if (tempMinP > Double.parseDouble(tokens[0])) {
            //             tempMinP = Double.parseDouble(tokens[0]);
            //             tempMinX = val.toString();
            //         } else { // If equal, randomly replace.
            //             Random r = new Random();
            //             if (r.nextBoolean()) {
            //                 tempMinP = Double.parseDouble(tokens[0]);
            //                 tempMinX = val.toString();
            //             }
            //         }
            //     }
            // }
            // minX.set(tempMinX);
            for (Text val: values) {
                context.write(key, val);
            }
        }
    }

    public Job run(String[] args, String y, int phenotype, int split) throws Exception {
        Configuration conf = new Configuration();
        conf.set("y", y);
        Job job = Job.getInstance(conf, "job manager");
        job.setJarByClass(BackwardManager.class);
        
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(DoubleWritable.class);
        job.setOutputValueClass(Text.class);
        job.setOutputFormatClass(NullOutputFormat.class);

        job.setMapperClass(LinRegMapper.class);
        job.setCombinerClass(MinSigReducer.class);
        job.setReducerClass(MinSigReducer.class);

        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path("Phenotype-" + phenotype + ".Split-" + split));
        
        job.waitForCompletion(true);
        return job;
    }
}