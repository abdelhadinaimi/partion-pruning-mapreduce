package bigdata;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
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
	
	public static void main(String[] args) throws Exception {
		
		Configuration conf = new Configuration();
//		conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
//		conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
		conf.set("fs.default.name", "hdfs://froment:9000");
		Job job = Job.getInstance(conf, "partitionOutput");
		job.setJarByClass(PartitionPruningOutput.class);

		job.setMapperClass(ContinentsOutputMapper.class);
		job.setNumReduceTasks(0);
		
		job.setInputFormatClass(TextInputFormat.class);
		TextInputFormat.setInputPaths(job, new Path(args[0]));
		
		job.setOutputFormatClass(ContinentsOutputFormat.class);
		
		job.setOutputKeyClass(CityKey.class);
		job.setOutputValueClass(Text.class);
	
		conf.set("outPath",args[1]);
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}
}
