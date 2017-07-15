package sems;

import java.io.IOException;
import java.io.BufferedReader;
import java.io.StringReader;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class AssignJobs {
	// Instance to map jobs.
	public static class TokenManager extends Mapper<Object, Text, Text, DoubleWritable> {

		public DoubleWritable time = new DoubleWritable(1.0);
		public Text groupName = new Text();
		
		// Maps the time taken for a job to their group name.
		// Parameters: key (Object) unused key object
		//             value (Text) file text
		//             context (Context) map/reduce object to interact with Hadoop system
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

			BufferedReader buff = new BufferedReader(new StringReader(value.toString()));
			
			String[] tokens;
			String line = null;
			while ((line = buff.readLine()) != null) {
				tokens = line.split(" ");
				if (tokens[1].split(";")[1].equals("E")) {
					String groupString = null;
					double start = 0;
					double end = 0;
					for (int i = 0; i < tokens.length; i++) {
						String[] subToken = tokens[i].split("=");
						if (subToken[0].equals("group")) {
							groupString = subToken[1] + "::" + tokens[0].split("/")[2] + "/" + tokens[0].split("/")[0];
						} else if (subToken[0].equals("start")) {
							start = Double.parseDouble(subToken[1]);
						} else if (subToken[0].equals("end")) {
							end = Double.parseDouble(subToken[1]);
						}
					}
					if (groupString != null) {
						groupName.set(groupString);
						time.set(end - start);
					}
				}
				System.out.println("<" + groupName.toString() + ", " + time.toString() + ">");
				context.write(groupName, time);
			}
		}
	}

	// Instance to reduce jobs.
	public static class DoubleSumReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

		private DoubleWritable result = new DoubleWritable();
		
		// Reduces the time taken for a job by their group name.
		// Paraemters: key (Text) group name
		//             values (Iterable<DoubleWritable>) times for each job
		//             context (Context) map/reduce object to interact with Hadoop system
		public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
			double sum = 0.0;
			for (DoubleWritable val: values) {
				sum += val.get();
			}
			result.set(sum);
			context.write(key, result);
		}
    }
    public void run(String[] args) throws Exception {
        Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "all time");
		job.setJarByClass(AssignJobs.class);
		job.setMapperClass(TokenManager.class);
		job.setCombinerClass(DoubleSumReducer.class);
		job.setReducerClass(DoubleSumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}