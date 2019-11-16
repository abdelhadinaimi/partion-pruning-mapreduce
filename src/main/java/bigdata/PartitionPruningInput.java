package bigdata;

import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class PartitionPruningInput {


	public static class ContinentsInputMapper extends Mapper<Text, Text, Text, Text> {
		@Override
		protected void map(Text key, Text value, Context context)
				throws IOException, InterruptedException {
			context.write(key,value);
		}
	}

	public static class ContinentsReducer extends Reducer<Text, Text, NullWritable, Text> {


	}
	

	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();
		conf.set(ContinentInputFormat.CONTINENTS_CONF, args[1]);
		Job job = Job.getInstance(conf, "partitionOutput");
		job.setJarByClass(PartitionPruningOutput.class);

		job.setReducerClass(ContinentsReducer.class);
		job.setNumReduceTasks(0);
		MultipleInputs.addInputPath(job, new Path(args[0]), ContinentInputFormat.class, ContinentsInputMapper.class);

		job.setOutputFormatClass(TextOutputFormat.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		FileOutputFormat.setOutputPath(job, new Path(args[2]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
