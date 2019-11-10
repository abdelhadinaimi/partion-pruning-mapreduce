package bigdata;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import com.google.protobuf.TextFormat;

public class PartitionPruningOutput {
		
	class ContinentsOutputMapper extends Mapper<Object, Text, CityKey, Text>{
		
		private CityKey key = new CityKey();
		private Text value = new Text();	
		@Override
		protected void map(Object key, Text value, Mapper<Object, Text, CityKey, Text>.Context context)
				throws IOException, InterruptedException {
			
		}
	}
	
	public void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.Di;
		conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
		Job job = Job.getInstance(conf, "partitionOutput");
		job.setJarByClass(PartitionPruningOutput.class);
		job.setNumReduceTasks(0);

		job.setMapperClass(ContinentsOutputMapper.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(ContinentsOutputFormat.class);
		
		job.setOutputKeyClass(CityKey.class);
		job.setOutputValueClass(Text.class);
	
		FileInputFormat.addInputPath(job, new Path(args[0]));
		ContinentsOutputFormat.setOutputPath(job, new Path(args[1]));
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}
}
