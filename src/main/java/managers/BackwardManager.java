package managers;

import utilities.ConfSet;
import utilities.Model;

import java.io.IOException;
import java.io.BufferedReader;
import java.io.StringReader;

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
import org.apache.commons.math3.distribution.TDistribution;
import org.apache.commons.math3.stat.regression.OLSMultipleLinearRegression;
import org.apache.commons.math3.util.FastMath;

public class BackwardManager {
    public static class LinRegMapper extends Mapper<Object, Text, Text, Text>{
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            BufferedReader buff = new BufferedReader(new StringReader(value.toString()));
            String line;
            String[] tokens;
            Configuration conf = context.getConfiguration();
            String mapKey = ConfSet.getY(conf);
            double[][] x;

            while ((line = buff.readLine()) != null) {
                tokens = line.split("\\t")[2].split(",");
                OLSMultipleLinearRegression regression = new OLSMultipleLinearRegression();

                // Converts yString to double[], and x tokens to double[][].
                // Combines x and y data and adds them to regression object.

                // TODO: Replace 1 with model + 1;
                // TODO: Fill for loop.
                String[] xStrings = new String[1];
                // for (int i = 0; i < xStrings.length; i++) {

                // }
                xStrings[xStrings.length - 1] = ConfSet.getXNewString(tokens);
                x = ConfSet.combineX(tokens, new double[tokens.length - 1][1]);
                regression.newSampleData(ConfSet.convertY(mapKey), x);
                try {
                    calculateSignificance(regression, conf, xStrings);
                } catch (Exception e) {
                    System.err.println("Invalid significance generated.");
                }
            }
            buff.close();
        }

        // Calculates significance of regressors.
        public static void calculateSignificance(OLSMultipleLinearRegression regression, Configuration conf, String[] xStrings) throws Exception {
            final double[] beta = regression.estimateRegressionParameters();
            final double[] standardErrors = regression.estimateRegressionParametersStandardErrors();
            final int residualdf = regression.estimateResiduals().length - beta.length;

            final TDistribution tdistribution = new TDistribution(residualdf);

            // beta.length - 1 to ignore new x.
            for (int i = 0; i < beta.length - 1; i++) {
                double tstat = beta[beta.length - 1] / standardErrors[beta.length - 1];
                double pvalue = tdistribution.cumulativeProbability(-FastMath.abs(tstat)) * 2;
                if (pvalue < 0.05) {
                    Model.setModel(conf.get("outPath"), xStrings[i]);
                }
            }
            Model.setModel(conf.get("outPath"), xStrings[xStrings.length - 1]);
        }
    }

    public static class MinSigReducer extends Reducer<Text, Text, Text, Text> {
        // private Text minX = new Text();

        public void reduce(Text key, Text values, Context context) throws IOException, InterruptedException {
            context.write(key, values);
        }
    }

    public Job run(String forwardFile, String y, String modelDir) throws Exception {
        Configuration conf = new Configuration();
        conf.set("y", y);
        conf.set("outPath", modelDir);
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

        FileInputFormat.addInputPath(job, new Path(forwardFile));
        FileOutputFormat.setOutputPath(job, new Path("/user/rchui2/temp"));
        
        job.waitForCompletion(true);
        return job;
    }
}