import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Question5 {
	public static class MapperSideJoinMapper extends Mapper<LongWritable, Text, Text, Text>{
		ArrayList<String> listOfBusinessIds = new ArrayList<String>();
		/***********************************************************************************
		 * The below method formulates logic for the mapper module.
		 * input types:key-longwritable ,value-text
		 * output types:key -text, value-text
		 * @see org.apache.hadoop.mapreduce.Mapper#map(KEYIN, VALUEIN, org.apache.hadoop.mapreduce.Mapper.Context)
		 ************************************************************************************/
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] dataRecord = value.toString().split("::");
			if (dataRecord.length > 23) {
				if (listOfBusinessIds.contains(dataRecord[2])) {
					context.write(new Text (dataRecord[8]), new Text (dataRecord[1]));
				}
			}
		}
		
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			super.setup(context);
			
			// driver code
			@SuppressWarnings("deprecation")
			Path[] localPaths = context.getLocalCacheFiles();
			for (Path filePath : localPaths) {
				String line = "";
				String fileName = filePath.getName();
				File file = new File(fileName + "");
				FileReader fr = new FileReader(file);
				BufferedReader br = new BufferedReader(fr);
				while ((line = br.readLine()) != null) {
					String[] dataRecord = line.split("::");
					if (dataRecord.length > 23) {
						//check for city stanford
						if (dataRecord[12].contains("Stanford")) {
							listOfBusinessIds.add(dataRecord[2]);
						}
					}
				}
				br.close();
			}
}
	}
	
	
	/**********************************************************************************
	 * The main method formulates driver logic for mapper side join. 
	 * @param args
	 * @throws Exception
	 **********************************************************************************/
	public static void main(String[] args) throws Exception {
		Configuration con = new Configuration();
		String[] otherArgs = new GenericOptionsParser(con, args).getRemainingArgs();
		
		if (otherArgs.length != 3) {
			System.err.println("Command line arguments: Question5 #Input File 1# #Input File 2# #Output File Directory#");
			System.exit(2);
		}
		
		@SuppressWarnings("deprecation")
		Job job = new Job(con, "Question5"); 
		job.setJarByClass(Question5.class); 
		job.setNumReduceTasks(0);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		MultipleInputs.addInputPath(job, new Path(otherArgs[0]), TextInputFormat.class, MapperSideJoinMapper.class);
		final String node = "hdfs://sandbox.hortonworks.com:8020";
		job.addCacheFile(new URI (node + "/user/hue/" + otherArgs[1]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));
		
		job.waitForCompletion(true);
	}
}
