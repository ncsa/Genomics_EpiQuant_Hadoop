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

public class DataCleaner {
    public static class TokenMapper extends Mapper<Object, Text, Text, LongWritable>{
        private final static LongWritable one = new LongWritable(1);

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
        public void map(Text key, LongWritable value, Context context) throws IOException, InterruptedException {
            context.write(key, NullWritable.get());
        }
    }

    public Job run(String inputPath, String outputDir) throws Exception {
        Configuration conf = new Configuration();
        conf.set("path", inputPath);
        Job job = Job.getInstance(conf, "data builder");
        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputDir));

        job.setJarByClass(DataCleaner.class);

        Configuration chainMapperConf = new Configuration(false);
        ChainMapper.addMapper(job, TokenMapper.class, Object.class, Text.class, Text.class, LongWritable.class, chainMapperConf);

        Configuration chainReducerConf = new Configuration(false);
        ChainReducer.setReducer(job, ElementReducer.class, Text.class, LongWritable.class, Text.class, LongWritable.class, chainReducerConf);
        ChainReducer.addMapper(job, ElementMapper.class, Text.class, LongWritable.class, Text.class, NullWritable.class, chainReducerConf);

        job.setNumReduceTasks(15);
        job.waitForCompletion(true);
        return job;
    }
}