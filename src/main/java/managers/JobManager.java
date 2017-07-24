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

/**
 * JobManager
 *     LinearRegressionMapper (Mapper) performs multiple linear regression over y, the current
 *                                     model, and each read in snp value.
 *     MinimumSignificanceReducer (Reducer) finds the minimum significance value of all values
 *                                          less than 0.05
 *     ModelMapper (Mapper) performance backwards significance testing and removes non-
 *                          significant model contributors. Writes out new model and the lowest
 *                          significance for this iteration.
 */
public class JobManager {
    public static class LinearRegressionMapper extends Mapper<Object, Text, Text, Text>{
        private double[][] xModel;
        private String model;
        private Set<String> xSet;

        /**
         * Reads in the current model.
         * Creates a set of values to ignore.
         * Parses the model string into values.
         */
        @Override
        public void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            model = getModel(conf);
            xSet = getModelSet(model);
            xModel = convertModel(model);
        }

        /**
         * For each snp value:
         *     1) Parses the snp value and adds it to the data set.
         *     2) Performs multiple linear regression over the data.
         *     3) Writes out a key, value pair if the signficance is less than 0.05.
         *         key = phenotype string, value = signficance + oldSnpModel + newSnp
         * Params:
         *     key (Object) generic file key.
         *     value (Text) text from mapper block that contains the snps.
         *     context (Context) object that contains job information.
         */
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

                    // Check if a model exists yet.
                    if (".".equals(model)) {
                        xModel =  new double[tokens.length - 1][1];
                    }
                    
                    // Parse the snp and phenotype data.
                    // Add the new set of data to the regression.
                    double[][] xTotal = ConfSet.combineX(tokens, xModel);
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

        /**
         * Gets the model string from the job configuration.
         * Params:
         *    conf (Configuration) job configuration that contains job information.
         */
        public String getModel(Configuration conf) {
            String model = conf.get("model");
            if (!".".equals(model)) {
                return model;
            } else {
                return ".";
            }
        }

        /**
         * Generates a set of snps already included in the model so we know what to exclude.
         * Params:
         *     model (String) the model in string form that needs to be parsed into values.
         */
        public Set<String> getModelSet(String model) {
            Set<String> xSet = new HashSet<String>();
            String[] snpStrings = model.split("\\r?\\n");
            for (int i = 0; i < snpStrings.length; i++) {
                xSet.add(snpStrings[i].split(",")[0]);
            }
            return xSet;
        }

        /**
         * Converts the model from a string into usable values for regression.
         * Params:
         *     model (String) the model in string form that needs to be parsed into values.
         */
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

        /**
         * Calculates the signficance (p-value) of a set of regressors.
         * If the value is significant (< 0.05) write out a key value pair.
         *     key = phenotype string, value = signficance + oldSnpModel + newSnp
         * Params:
         *     regression (OLSMultipleLinearRegression) performs multiple linear regression.
         *     context (Context) object that contains job information.
         *     mapKey (String) phenotype string.
         *     tokens (String[]) tokenized snp values.
         *     model (String) model string.
         */
        public void calculateSignificance(OLSMultipleLinearRegression regression, Context context, String mapKey, String[] tokens, String model) throws Exception {
            // Compute p-value
            final double[] beta = regression.estimateRegressionParameters();
            final double[] standardErrors = regression.estimateRegressionParametersStandardErrors();
            final int residualDF = regression.estimateResiduals().length - beta.length;
            final TDistribution tDistribution = new TDistribution(residualDF);
            double tstat = beta[beta.length - 1] / standardErrors[beta.length - 1];
            double pValue = tDistribution.cumulativeProbability(-FastMath.abs(tstat)) * 2;

            // Check for signficance. Write out key, value pair.
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

        /**
         * Finds the minimum p-value and its associated model.
         * Params:
         *     key (Text) phenotype string.
         *     values (Iterable<Text>) all qualified snp p-values and their models.
         *     context (Context) object that contains job information.
         */
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

        /**
         * Sets up multiple outputs for ModelMapper.
         * Params:
         *     context (Context) object that contains job information.
         */
        @Override
        public void setup(Context context) throws IOException, InterruptedException {
            mos = new MultipleOutputs<Text, NullWritable>(context);
        }

        /**
         * Performs backward elimination of the snp model; excluding non-significant values.
         * Params:
         *     key (Text) phenotype string.
         *     value (Text) significance and model string.
         *     context (Context) object that contains job information.
         */
        public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            String baseDir = conf.get("baseDir");

            // Parse the model strings for the model values.
            double[] y = ConfSet.convertY(key.toString());
            String[] values = value.toString().split("\\t");
            String[] xStrings = values[1].split("\\r?\\n");

            // Convert x's to appropriate orientation and type for compatability with 
            // OLSMulitpleLinearRegression
            int xLength = xStrings[0].split(",").length;
            double[][] x = new double[xLength - 1][xStrings.length];
            for (int i = 0; i < xStrings.length; i++) {
                String[] xValues = xStrings[i].split(",");
                for (int j = 1; j < xValues.length; j++) {
                    x[j - 1][i] = Double.parseDouble(xValues[j]);
                }
            }

            // Calculate significance (p-value) for each snp in the model.
            OLSMultipleLinearRegression regression = new OLSMultipleLinearRegression();
            regression.newSampleData(y, x);
            try {
                setModel(regression, baseDir, xStrings);
            } catch (Exception e) {
                System.err.println("Invalid significance generated.");
            }

            mos.write("significance", new Text(values[0]), NullWritable.get());
        }

        /**
         * Computes the p-value for each regressor currently in the model.
         * Excludes values that do not reach signficance.
         * Params:
         *     regression (OLSMultipleLinearRegression) 
         *     baseDir (String) base directory to write to (DEPRECTATED).
         *     xStrings (String[]) strings for each of the models.
         * 
         * TODO: Remove baseDir from setModel().
         */
        public void setModel(OLSMultipleLinearRegression regression, String baseDir, String[] xStrings) throws Exception {
            // Compute p-values for each regressor.
            final double[] beta = regression.estimateRegressionParameters();
            final double[] standardErrors = regression.estimateRegressionParametersStandardErrors();
            final int residualDF = regression.estimateResiduals().length - beta.length;
            final TDistribution tDistribution = new TDistribution(residualDF);

            // Write out each snp that is significant.
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

    /**
     * Creates a new map reduce job and defines the map and reduce tasks, and defines the input and
     * output files and file formats.
     * Params:
     *     jobPath (String) input path for map reduce job.
     *     y (String) phenotype to consider for this job.
     *     model (String) current model to include in this job.
     *     baseDir (String) directory to write out to. (DEPRECATED)
     *     phenotype (int) the phenotype's assigned number.
     *     split (int) the iteration the algorithm is on.
     * 
     * TODO: Remove baseDir from run().
     */
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
        
        job.submit();
        return job;
    }
}