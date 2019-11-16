package bigdata;

import java.io.BufferedReader;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

public class ContinentInputFormat extends FileInputFormat<Text,Text>{
	public static String CONTINENTS_CONF = "continents.conf";
	public static String CONTINENTS_INPUT_CONF = "continents.input.conf";

	
	@Override
	public RecordReader<Text, Text> createRecordReader(InputSplit split, TaskAttemptContext context)
			throws IOException, InterruptedException {
		
		ContinentRecordReader recordReader = new ContinentRecordReader();
		recordReader.initialize(split, context);
		return recordReader;
	}
	
	public class ContinentRecordReader extends RecordReader<Text, Text>{
		
		private LineRecordReader lineReader = null;
		private Text key = null;  
		private Text value = null;
		@Override
		public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
			key = new Text();
			value = new Text();
			
			String[] continents = context.getConfiguration().get(CONTINENTS_CONF).split(",");
			FileSplit fs = (FileSplit) split;
			String[] locSplited = fs.getPath().toString().split("/");
			Boolean contains = false;
			for(String conti : continents){
				if(locSplited[locSplited.length-1].contains(conti)){
					contains = true;
					break;
				}
			}
			if(contains){
				lineReader = new LineRecordReader();
				lineReader.initialize(split, context);				
			}
		}

		@Override
		public boolean nextKeyValue() throws IOException, InterruptedException {
			if(lineReader == null || !lineReader.nextKeyValue()){
				return false;
			} 
			String line = lineReader.getCurrentValue().toString();
			key.set(line);
			value.set(line);
			return true;
		}

		@Override
		public Text getCurrentKey() throws IOException, InterruptedException {
			return key;
		}

		@Override
		public Text getCurrentValue() throws IOException, InterruptedException {
			return value;
		}

		@Override
		public float getProgress() throws IOException, InterruptedException {
			return lineReader == null ? 1 : lineReader.getProgress();
		}

		@Override
		public void close() throws IOException {
			if(lineReader != null){
				lineReader.close();				
			}
		}	
	}

	public static void setInputPath(Job job, Path path) {
		try {
			path = path.getFileSystem(job.getConfiguration()).makeQualified(path);
		} catch (IOException e) {
			// Throw the IOException as a RuntimeException to be compatible with
			// MR1
			throw new RuntimeException(e);
		}
		job.getConfiguration().set(CONTINENTS_INPUT_CONF, path.toString());
		
	}
	
	public class ContinentInputSplit extends InputSplit implements Writable{
		
		private String continent = null;
		
		public ContinentInputSplit(String continent) {}
		
		@Override
		public long getLength() throws IOException, InterruptedException {
			return 0;
		}

		@Override
		public String[] getLocations() throws IOException, InterruptedException {
			return new String[]{ continent };
		}


		public void write(DataOutput out) throws IOException {
			out.writeUTF(continent);
			
		}

		public void readFields(DataInput in) throws IOException {
			continent = in.readUTF();
			
		}
		
	}
}