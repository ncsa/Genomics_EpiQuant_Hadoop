package managers;

import utilities.ConfSet;

import java.io.IOException;
import java.io.BufferedReader;
import java.io.StringReader;
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
import org.apache.commons.math3.util.FastMath;
import org.apache.commons.math3.distribution.TDistribution;
import org.apache.commons.math3.stat.regression.OLSMultipleLinearRegression;

public class ForwardManager {
    public static class LinRegMapper extends Mapper<Object, Text, Text, Text>{
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            BufferedReader buff = new BufferedReader(new StringReader(value.toString()));
            String line;
            String[] tokens;
            Configuration conf = context.getConfiguration();
            String mapKey = ConfSet.getY(conf);
            double[][] x;

            while ((line = buff.readLine()) != null) {
                tokens = line.split("\\t");
                OLSMultipleLinearRegression regression = new OLSMultipleLinearRegression();

                // Converts yString to double[], and x tokens to double[][].
                // Combines x and y data and adds them to regression object.
                x = ConfSet.combineX(tokens, new double[tokens.length - 1][1]);
                regression.newSampleData(ConfSet.convertY(mapKey), x);
                try {
                    calculateSignificance(regression, context, mapKey, tokens);
                } catch (Exception e) {
                    System.err.println("Invalid significance generated.");
                }
            }
            buff.close();
        }

        // Calculates significance of regressors.
        public static void calculateSignificance(OLSMultipleLinearRegression regression, Context context, String mapKey, String[] tokens) throws Exception {
            final double[] beta = regression.estimateRegressionParameters();
            final double[] standardErrors = regression.estimateRegressionParametersStandardErrors();
            final int residualdf = regression.estimateResiduals().length - beta.length;

            final TDistribution tdistribution = new TDistribution(residualdf);

            double tstat = beta[beta.length - 1] / standardErrors[beta.length - 1];
            double pvalue = tdistribution.cumulativeProbability(-FastMath.abs(tstat)) * 2;
            if (pvalue < 0.05) {
                context.write(new Text(mapKey), new Text(Double.toString(pvalue) + "\t" + ConfSet.getXNewString(tokens)));
            }
        }
    }

    public static class MinSigReducer extends Reducer<Text, Text, Text, Text> {
        private Text minX = new Text();

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            double tempMinP = 0.05;
            String tempMinX = "";
            String[] tokens;

            for (Text val: values) {
                tokens = val.toString().split("\\t");
                // If current is less do nothing.
                if (!(tempMinP < Double.parseDouble(tokens[0]))) { 
                    // If current is greater, replace.
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
            minX.set(tempMinX);
            context.write(key, minX);
        }
    }

    public Job run(String[] args, String y, int phenotype, int split) throws Exception {
        Configuration conf = new Configuration();
        conf.set("y", y);
        Job job = Job.getInstance(conf, "job manager");
        job.setJarByClass(ForwardManager.class);
        
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(DoubleWritable.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(LinRegMapper.class);
        job.setCombinerClass(MinSigReducer.class);
        job.setReducerClass(MinSigReducer.class);

        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path("Phenotype-" + phenotype + ".Split-" + split));
        
        job.waitForCompletion(true);
        return job;
    }
}