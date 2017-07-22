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

public class JobManager {
    public static class LinearRegressionMapper extends Mapper<Object, Text, Text, Text>{
        private double[][] xModel;
        private String model;
        private Set<String> xSet;

        @Override
        public void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            model = getModel(conf);
            xSet = getModelSet(model);
            xModel = convertModel(model);
        }

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            BufferedReader buff = new BufferedReader(new StringReader(value.toString()));
            String line;
            String[] tokens;
            Configuration conf = context.getConfiguration();
            String mapKey = ConfSet.getY(conf);

            while ((line = buff.readLine()) != null) {
                tokens = line.split("\\t");
                if (!xSet.contains(tokens[0])) {
                    OLSMultipleLinearRegression regression = new OLSMultipleLinearRegression();

                    // Converts yString to double[], and x tokens to double[][].
                    // Combines x and y data and adds them to regression object.
                    if (".".equals(model)) {
                        xModel =  new double[tokens.length - 1][1];
                    }
                    double[][] xTotal = ConfSet.combineX(tokens, xModel); // Make sure to +1 index
                    regression.newSampleData(ConfSet.convertY(mapKey), xTotal);
                    try {
                        calculateSignificance(regression, context, mapKey, tokens, model);
                    } catch (Exception e) {
                        System.err.println("Invalid significance generated.");
                    }
                }
            }
            buff.close();
        }

        public String getModel(Configuration conf) {
            String model = conf.get("model");
            if (!".".equals(model)) {
                return model;
            } else {
                return ".";
            }
        }

        public Set<String> getModelSet(String model) {
            Set<String> xSet = new HashSet<String>();
            String[] snpStrings = model.split("\\r?\\n");
            for (int i = 0; i < snpStrings.length; i++) {
                xSet.add(snpStrings[i].split(",")[0]);
            }
            return xSet;
        }

        public double[][] convertModel(String model) {
            String[] snpStrings = model.split("\\r?\\n");
            int snpLength = snpStrings[0].split(",").length;
            double[][] x = new double[snpLength - 1][snpStrings.length + 1];
            for (int i = 0; i < snpStrings.length; i++) {
                String[] snpValues = snpStrings[i].split(",");
                for (int j = 1; j < snpValues.length; j++) {
                    x[j - 1][i] = Double.parseDouble(snpValues[j]);
                }
            }
            return x;
        }

        // Calculates significance of regressors.
        public void calculateSignificance(OLSMultipleLinearRegression regression, Context context, String mapKey, String[] tokens, String model) throws Exception {
            final double[] beta = regression.estimateRegressionParameters();
            final double[] standardErrors = regression.estimateRegressionParametersStandardErrors();
            final int residualDF = regression.estimateResiduals().length - beta.length;

            final TDistribution tDistribution = new TDistribution(residualDF);

            double tstat = beta[beta.length - 1] / standardErrors[beta.length - 1];
            double pValue = tDistribution.cumulativeProbability(-FastMath.abs(tstat)) * 2;
            if (pValue < 0.05) {
                if (".".equals(model)) {
                    context.write(new Text(mapKey), new Text(Double.toString(pValue) + "\t" + ConfSet.getXNewString(tokens)));
                } else {
                    context.write(new Text(mapKey), new Text(Double.toString(pValue) + "\t" + model + "\n" + ConfSet.getXNewString(tokens)));
                }
            }
        }
    }

    public static class MinimumSignificanceReducer extends Reducer<Text, Text, Text, Text> {
        private Text minX = new Text();

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            double tempMinP = 0.05;
            String tempMinX = "";
            String[] tokens;

            for (Text val: values) {
                tokens = val.toString().split("\\t");
                double significance = Double.parseDouble(tokens[0]);
                // If current is less do nothing.
                if (!(tempMinP < significance)) { 
                    // If current is greater, replace.
                    if (tempMinP > significance) {
                        tempMinP = significance;
                        tempMinX = val.toString();
                    } else { // If equal, randomly replace.
                        Random r = new Random();
                        if (r.nextBoolean()) {
                            tempMinP = significance;
                            tempMinX = val.toString();
                        }
                    }
                }
            }
            minX.set(tempMinX);
            context.write(key, minX);
        }
    }

    public static class ModelMapper extends Mapper<Text, Text, Text, NullWritable>{
        private MultipleOutputs<Text, NullWritable> mos;

        @Override
        public void setup(Context context) throws IOException, InterruptedException {
            mos = new MultipleOutputs<Text, NullWritable>(context);
        }

        public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            String baseDir = conf.get("baseDir");

            // Get starter values.
            double[] y = ConfSet.convertY(key.toString());
            String[] values = value.toString().split("\\t");
            String[] xStrings = values[1].split("\\r?\\n");

            // Convert x's to appropriate orientation and type.
            int xLength = xStrings[0].split(",").length;
            double[][] x = new double[xLength - 1][xStrings.length];
            for (int i = 0; i < xStrings.length; i++) {
                String[] xValues = xStrings[i].split(",");
                for (int j = 1; j < xValues.length; j++) {
                    x[j - 1][i] = Double.parseDouble(xValues[j]);
                }
            }

            // Calculate significance.
            OLSMultipleLinearRegression regression = new OLSMultipleLinearRegression();
            regression.newSampleData(y, x);
            try {
                setModel(regression, baseDir, xStrings);
            } catch (Exception e) {
                System.err.println("Invalid significance generated.");
            }

            mos.write("significance", new Text(values[0]), NullWritable.get());
        }

        // Set model based off of significance of regressors.
        public void setModel(OLSMultipleLinearRegression regression, String baseDir, String[] xStrings) throws Exception {
            final double[] beta = regression.estimateRegressionParameters();
            final double[] standardErrors = regression.estimateRegressionParametersStandardErrors();
            final int residualDF = regression.estimateResiduals().length - beta.length;

            final TDistribution tDistribution = new TDistribution(residualDF);

            for (int i = 1; i < beta.length - 1; i++) {
                double tstat = beta[i] / standardErrors[i];
                double pValue = tDistribution.cumulativeProbability(-FastMath.abs(tstat)) * 2;
                if (pValue < 0.05) {
                    mos.write("model", new Text(xStrings[i - 1]), NullWritable.get());
                }
            }
            mos.write("model", new Text(xStrings[beta.length - 2]), NullWritable.get());
        }
    }

    public Job run(String jobPath, String y, String model, String baseDir, int phenotype, int split) throws Exception {
        Configuration conf = new Configuration();
        conf.set("baseDir", baseDir);
        conf.set("model", model);
        conf.set("y", y);
        Job job = Job.getInstance(conf, "job manager");
        FileInputFormat.addInputPath(job, new Path(jobPath));
        FileOutputFormat.setOutputPath(job, new Path("Phenotype-" + phenotype + ".Split-" + split));

        job.setJarByClass(JobManager.class);

        Configuration chainMapperConf = new Configuration(false);
        ChainMapper.addMapper(job, LinearRegressionMapper.class, Object.class, Text.class, Text.class, Text.class, chainMapperConf);

        Configuration chainReducerConf = new Configuration(false);
        ChainReducer.setReducer(job, MinimumSignificanceReducer.class, Text.class, Text.class, Text.class, Text.class, chainReducerConf);
        ChainReducer.addMapper(job, ModelMapper.class, Text.class, Text.class, Text.class, NullWritable.class, chainReducerConf);

        MultipleOutputs.addNamedOutput(job, "significance", TextOutputFormat.class, Text.class, NullWritable.class);
        MultipleOutputs.addNamedOutput(job, "model", TextOutputFormat.class, Text.class, NullWritable.class);
        
        job.setNumReduceTasks(5);
        job.waitForCompletion(true);
        return job;
    }
}