package bigdata;

import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;

public class ContinentsOutputFormat extends OutputFormat<CityKey, Text> {
	
	final static String[] CONTINENTS = { "NA", "SA", "AF", "EU", "AS", "AU" };
	final static HashMap<String,String> CONTINENT_FROM_COUNTRY = new HashMap<String,String>();
	
	static {
		CONTINENT_FROM_COUNTRY.put("fr", "EU");
		CONTINENT_FROM_COUNTRY.put("dz", "AF");
		CONTINENT_FROM_COUNTRY.put("de", "EU");
		CONTINENT_FROM_COUNTRY.put("au", "AU");
	}
	
	@Override
	public RecordWriter<CityKey, Text> getRecordWriter(TaskAttemptContext context)
			throws IOException, InterruptedException {
		return new ContinentsRecordWriter(context.getConfiguration());
	}

	@Override
	public void checkOutputSpecs(JobContext context) throws IOException, InterruptedException {
	}

	@Override
	public OutputCommitter getOutputCommitter(TaskAttemptContext context) throws IOException, InterruptedException {
		return (new NullOutputFormat<Text, Text>()).getOutputCommitter(context);
	}

	public static class ContinentsRecordWriter extends RecordWriter<CityKey, Text> {

		
		private HashMap<Integer, Jedis> jedisMap = new HashMap<Integer, Jedis>();
		 
		public ContinentsRecordWriter(Configuration conf) {
			for (String c : CONTINENTS) {
				FileSystem fs = FileSystem.get(conf);
				Path path = new Path(conf.get(FileOutputFormat.OUTDIR)+"/"+c);
				FSDataInputStream inputStream = fs.open(path);

				FSDataOutputStream dos = dfs.create(path);
//				Cassical output stream usage
//				outputStream.writeBytes(fileContent);
//				outputStream.close();
			}	
		}

		@Override
		public void write(CityKey key, Text value) throws IOException, InterruptedException {
			// TODO Auto-generated method stub

		}

		@Override
		public void close(TaskAttemptContext context) throws IOException, InterruptedException {
			// TODO Auto-generated method stub

		}

	}

	public static void setOutputPath(Job job, Path outputDir) {
		try {
			outputDir = outputDir.getFileSystem(job.getConfiguration()).makeQualified(outputDir);
		} catch (IOException e) {
			// Throw the IOException as a RuntimeException to be compatible with
			// MR1
			throw new RuntimeException(e);
		}
		job.getConfiguration().set(FileOutputFormat.OUTDIR, outputDir.toString());
	}

}
