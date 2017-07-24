package managers;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.BufferedReader;
import java.io.StringReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.chain.ChainReducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * DataBuilder
 *     TokenMapper (Mapper) Reads in snps and emits all possible single and pair-wise interactions.
 *     ElementReducer (Reducer) Takes in all common keys and pipes the output to the ElementMapper.
 *     ElementMapper (Mapper) Computes pair-wise values 
 */
public class DataBuilder {
    public static class TokenMapper extends Mapper<Object, Text, Text, Text>{
        private String pathString;
        private Path path;
        private FileSystem fs;

        /**
         * Gets the path to the previous model file and sets up the filesystem reader.
         * Params:
         *     context (Context) object that contains job information.
         */
        @Override
        public void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            pathString = conf.get("path");
            path = new Path(pathString);
            fs = FileSystem.get(conf);
        }

        /**
         * Reads in values from its block and from the snp file.
         * Pairs all possible combinations from its block and the snp file, outputing them as
         * key value pairs.
         * Params:
         *     key (Object) generic key value.
         *     value (Text) block assigned to this mapper from the snp file.
         *     context (Context) object that contains job information.
         */
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            BufferedReader fileBuff = new BufferedReader(new InputStreamReader(fs.open(path)));
            String fileLine;

            while ((fileLine = fileBuff.readLine()) != null) {
                BufferedReader valueBuff = new BufferedReader(new StringReader(value.toString()));
                String valueLine;
                while ((valueLine = valueBuff.readLine()) != null) {
                    if (!valueLine.equals(fileLine)) {
                        context.write(new Text(fileLine), new Text(valueLine));
                    }
                }
            }
        }
    }

    public static class ElementReducer extends Reducer<Text, Text, Text, Text> {

        /**
         * Takes all common keys and emits them as is to ElementMapper.
         * Params:
         *     key (Text) one of the snp strings.
         *     values (Iterable<Text>) all other snp strings.
         *     context (Context) object that contains job information.
         */
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text value: values) {
                context.write(key, value);
            }
        }
    }

    public static class ElementMapper extends Mapper<Text, Text, Text, NullWritable>{

        /**
         * For each key, value pair:
         *     1) Parses the snp strings into values.
         *     2) Assigns combined snp interaction names
         *     3) Writes out the snp interaction name and the single snp interaction to file.
         * Params:
         *     key (Text) one of the snp strings to compute snp interaction.
         *     value (Text) another snp string to compute snp interaction.
         *     context (Context) object that contains job information.
         */
        public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            // Parse key and value for values.
            String[] fileTokens = key.toString().split("\\t");
            String[] valueTokens = value.toString().split("\\t");
            double[] outDoubles = new double[fileTokens.length - 1];
            for (int i = 0; i < outDoubles.length; i++) {
                outDoubles[i] = Double.parseDouble(fileTokens[i + 1]) * Double.parseDouble(valueTokens[i + 1]);
            }

            // Produce output string which contains alaphebetically lower first and higher second.
            // ie. x1 and x2 will always be x1:::x2 and never x2:::x1.
            String outString;
            if (fileTokens[0].compareTo(valueTokens[0]) < 0) {
                outString = fileTokens[0] + ":::" + valueTokens[0];
            } else if (fileTokens[0].compareTo(valueTokens[0]) > 0) {
                outString = valueTokens[0] + ":::" + fileTokens[0];
            } else {
                outString = fileTokens[0] + ":::" + valueTokens[0];
            }

            // Construct output string and write both the interaction and key snp string to file.
            for (int i = 0; i < outDoubles.length; i++) {
                outString += "\t" + outDoubles[i];
            }
            context.write(new Text(outString), NullWritable.get());
            context.write(key, NullWritable.get());
        }
    }

    /**
     * Creates a new map reduce job and defines the map and reduce tasks, and defines the input and
     * output files and file formats.
     * Params:
     *     inputPath (String) the path to read data from.
     *     outputDir (String) path to write data to.
     */
    public Job run(String inputPath, String outputDir) throws Exception {
        Configuration conf = new Configuration();
        conf.set("path", inputPath);
        Job job = Job.getInstance(conf, "data builder");
        
        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputDir));

        job.setJarByClass(DataBuilder.class);

        Configuration chainMapperConf = new Configuration(false);
        ChainMapper.addMapper(job, TokenMapper.class, Object.class, Text.class, Text.class, Text.class, chainMapperConf);

        Configuration chainReducerConf = new Configuration(false);
        ChainReducer.setReducer(job, ElementReducer.class, Text.class, Text.class, Text.class, Text.class, chainReducerConf);
        ChainReducer.addMapper(job, ElementMapper.class, Text.class, Text.class, Text.class, NullWritable.class, chainReducerConf);

        job.setNumReduceTasks(5);
        job.waitForCompletion(true);
        return job;
    }
}