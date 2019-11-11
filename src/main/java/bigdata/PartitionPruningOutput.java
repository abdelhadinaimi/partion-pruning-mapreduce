package bigdata;

import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class PartitionPruningOutput {

	final static String[] CONTINENTS = { "NA", "SA", "AF", "EU", "AS", "AU" };
	final static HashMap<String, String> CONTINENT_FROM_COUNTRY = new HashMap<String, String>();

	static {
		CONTINENT_FROM_COUNTRY.put("fr", "EU");
		CONTINENT_FROM_COUNTRY.put("tn", "AF");
		CONTINENT_FROM_COUNTRY.put("dz", "AF");
		CONTINENT_FROM_COUNTRY.put("de", "EU");
		CONTINENT_FROM_COUNTRY.put("au", "AU");
	}

	public static class ContinentsOutputMapper extends Mapper<Object, Text, Text, Text> {

		private Text outKey = new Text();
		private Text outValue = new Text();

		protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String[] tokens = value.toString().split(",");
			String continent;
			if (!tokens[0].equals("") && (continent = CONTINENT_FROM_COUNTRY.get(tokens[0].toLowerCase())) != null) {
				outKey.set(continent);
				outValue.set(value.toString());
				context.write(outKey, outValue);
			}
		}
	}

	public static class ContinentsReducer extends Reducer<Text, Text, NullWritable, Text> {

		private MultipleOutputs<NullWritable, Text> mos;

		@Override
		public void setup(Context context) {
			mos = new MultipleOutputs<NullWritable, Text>(context);
		}

		public void reduce(Text key, Iterable<Text> values, Context context) {
			for (Text value : values) {
				try {
					mos.write(key.toString(), null, value, key.toString());
				} catch (IOException e) {
					e.printStackTrace();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}

		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			mos.close();
		}
	}

	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();

		Job job = Job.getInstance(conf, "partitionOutput");
		job.setJarByClass(PartitionPruningOutput.class);

		job.setMapperClass(ContinentsOutputMapper.class);
		job.setReducerClass(ContinentsReducer.class);
		job.setNumReduceTasks(1);

		job.setInputFormatClass(TextInputFormat.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		for (String conti : CONTINENTS) {
			MultipleOutputs.addNamedOutput(job, conti, TextOutputFormat.class, NullWritable.class, Text.class);
		}

		TextInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}
}
