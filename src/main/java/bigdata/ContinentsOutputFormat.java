package bigdata;

import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
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
	final static HashMap<String, String> CONTINENT_FROM_COUNTRY = new HashMap<String, String>();

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

		private HashMap<String, FSDataOutputStream> continentToStreamMap = new HashMap<String, FSDataOutputStream>();
		FileSystem fs;

		public ContinentsRecordWriter(Configuration conf) {
			try {
				fs = FileSystem.get(conf);
			} catch (IOException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
			for (String c : CONTINENTS) {
				try {
					Path path = new Path(conf.get("outPath")+"/"+c);
						
					if (!fs.exists(path))
						fs.createNewFile(path);
					continentToStreamMap.put(c, fs.create(path));
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}

		@Override
		public void write(CityKey key, Text value) throws IOException, InterruptedException {
			// TODO Auto-generated method stub

		}

		@Override
		public void close(TaskAttemptContext context) throws IOException, InterruptedException {
			fs.close();
			for(FSDataOutputStream fsData : continentToStreamMap.values()){
				fsData.close();
			}
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
