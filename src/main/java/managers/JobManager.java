package managers;

import utilities.ConfSet;

import java.io.IOException;
import java.io.BufferedReader;
import java.io.StringReader;
import java.util.Set;
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
import org.apache.commons.math3.stat.regression.SimpleRegression;
import org.apache.commons.math.stat.regression.MultipleLinearRegression;

public class JobManager {
    public static class LinRegMapper extends Mapper<Object, Text, Text, Text>{
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            BufferedReader buff = new BufferedReader(new StringReader(value.toString()));
            String line;
            String[] tokens;
            Configuration conf = context.getConfiguration();
            String mapKey = ConfSet.getY(conf);

            while ((line = buff.readLine()) != null) {
                tokens = line.split("\\t");
                SimpleRegression regression = new SimpleRegression();

                // Converts yString, and x tokens to double[].
                // Combines x and y data and adds them to regression object.
                regression.addData(combineData(convertXNew(tokens), ConfSet.convertY(mapKey)));
                try {
                    double significance = regression.getSignificance();
                    if (significance < 0.05) {
                        context.write(new Text(mapKey), new Text(Double.toString(significance) + "\t" + getXNewString(tokens)));
                    }
                } catch (Exception e) {
                    System.err.println("Invalid significance generated.");
                }
            }
            buff.close();
        }

        public static double[] convertXNew(String[] tokens) {
            double[] xNew = new double[tokens.length - 1];
            for (int i = 1; i < tokens.length; i++) {
                xNew[i - 1] = Double.parseDouble(tokens[i]);
            }
            return xNew;
        }

        public static String getXNewString(String[] tokens) {
            String xOut = tokens[0];
            for (int i = 1; i < tokens.length; i++) {
                xOut += "," + tokens[i];
            }
            return xOut;
        }

        public static double[][] combineData(double[] x, double[] y) {
            double[][] data = new double[x.length][2];
            for (int i = 0; i < x.length; i++) {
                data[i] = new double[]{x[i], y[i]};
            }
            return data;
        }
    }

    public static class MinSigReducer extends Reducer<Text, Text, DoubleWritable, Text> {
        private Text minX = new Text();
        private DoubleWritable minP = new DoubleWritable();

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            double tempMinP = 0.05;
            String tempMinX = "";
            String[] tokens;

            for (Text val: values) {
                tokens = val.toString().split("\\t");
                // If current is greater do nothing.
                if (!(tempMinP < Double.parseDouble(tokens[0]))) { 
                    // If current is less than, replace.
                    if (tempMinP > Double.parseDouble(tokens[0])) {
                        tempMinP = Double.parseDouble(tokens[0]);
                        tempMinX = val.toString();
                    } else { // If equal, randomly replace.
                        Random r = new Random();
                        if (r.nextBoolean()) {
                            tempMinP = Double.parseDouble(tokens[0]);
                            tempMinX = val.toString();
                        }
                    }
                }
            }
            tokens = tempMinX.split("\\t");
            minX.set(tokens[1]);
            minP.set(tempMinP);
            context.write(minP, minX);
        }
    }

    public Job run(String[] args, String y, int phenotype, int split) throws Exception {
        Configuration conf = new Configuration();
        conf.set("y", y);
        Job job = Job.getInstance(conf, "job manager");
        job.setJarByClass(JobManager.class);
        
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(DoubleWritable.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(LinRegMapper.class);
        job.setCombinerClass(MinSigReducer.class);
        job.setReducerClass(MinSigReducer.class);

        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path("Phenotype-" + phenotype + ".Split-" + split));
        
        job.submit();
        return job;
    }
}