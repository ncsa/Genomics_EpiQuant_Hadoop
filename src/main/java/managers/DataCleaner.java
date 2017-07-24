package managers;

import java.io.IOException;
import java.io.BufferedReader;
import java.io.StringReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
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
 *     Performing a word count over lines to get unique lines.
 */
public class DataCleaner {
    public static class TokenMapper extends Mapper<Object, Text, Text, LongWritable>{
        private final static LongWritable one = new LongWritable(1);

        /**
         * Reads in lines from mapper block and writes line as key, and 1 as value.
         * Params:
         *     key (Object) generic file key.
         *     value (Text) text for this mapper block.
         *     context (Context) object that contains job information.
         */
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            BufferedReader buff = new BufferedReader(new StringReader(value.toString()));
            String line;

            while ((line = buff.readLine()) != null) {
                context.write(new Text(line), one);
            }
        }
    }

    public static class ElementReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
        private LongWritable result = new LongWritable();

        /**
         * Sums over the number of each line.
         * Params:
         *     key (Text) snp line string.
         *     values (Iterable<LongWritable>) number of appearances of the line (key).
         *     context (Context) object that contains job information.
         */
        public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            long sum = 0;
            for (LongWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static class ElementMapper extends Mapper<Text, LongWritable, Text, NullWritable>{
        
        /**
         * Writes each unique key to a file.
         * Params:
         *     key (Text) unique snp line.
         *     value (LongWritable) number of appearances for that line.
         *     context (Context) object that contains job information.
         */
        public void map(Text key, LongWritable value, Context context) throws IOException, InterruptedException {
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

        job.setJarByClass(DataCleaner.class);

        Configuration chainMapperConf = new Configuration(false);
        ChainMapper.addMapper(job, TokenMapper.class, Object.class, Text.class, Text.class, LongWritable.class, chainMapperConf);

        job.setCombinerClass(ElementReducer.class);

        Configuration chainReducerConf = new Configuration(false);
        ChainReducer.setReducer(job, ElementReducer.class, Text.class, LongWritable.class, Text.class, LongWritable.class, chainReducerConf);
        ChainReducer.addMapper(job, ElementMapper.class, Text.class, LongWritable.class, Text.class, NullWritable.class, chainReducerConf);

        job.waitForCompletion(true);
        return job;
    }
}