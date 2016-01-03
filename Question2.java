import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Question2 {
	/**********************************************************************************
	 * The below class formulates logic for the mapper/reducer module to list 
	 *  each business Id that are located in “Palo Alto” using the full_address column as thefilter column. 
	 **********************************************************************************/
	public static class BusinessIdMapper extends Mapper<LongWritable, Text, Text, IntWritable>{

		private final static IntWritable one = new IntWritable(1);
		/***********************************************************************************
		 * The below method formulates logic for the mapper module where, input key is of type long,
		 * value is of type text(string) and context specifies the output key value pair.
		 * (non-Javadoc)
		 * input types:key-longwritable ,value-text
		 * output types:key -text, value-intwritable
		 * @see org.apache.hadoop.mapreduce.Mapper#map(KEYIN, VALUEIN, org.apache.hadoop.mapreduce.Mapper.Context)
		 ************************************************************************************/
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] dataRecord = value.toString().split("::");
			if (dataRecord.length > 23){
				//checking business type and check if address contains palo alto.
				if("business".compareTo(dataRecord[22])== 0 && dataRecord[3].contains("Palo Alto")== true){

					Text outputKey = new Text (dataRecord[2]);
					//outputting business id as key and count as value
					context.write(outputKey, one);
				}
			}
			
		}
	}
	/***********************************************************************************
	 * The below class formulates the reducer logic. 
	 * input types: key-text ,value-intwritable
	 * output types: key -text, value-nullwritable
	 ************************************************************************************/
	public static class BusinessIdReducer extends Reducer<Text,IntWritable,Text,NullWritable> {
		private NullWritable outputValue = NullWritable.get();

		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			//outputting business id as key and null as value.
			context.write(key, outputValue); 
		}
	}
	/**********************************************************************************
	 * The main method formulates driver logic for the listing business id's located in palo alto driver setup.
	 * @param args
	 * @throws Exception
	 **********************************************************************************/
	public static void main(String[] args) throws Exception {
		Configuration con = new Configuration();
		String[] otherArgs = new GenericOptionsParser(con, args).getRemainingArgs();
		
		if (otherArgs.length != 2) {
			System.err.println("Command line arguments: Question2 #Input File# #Output File#>");
			System.exit(2);
		}

		
		@SuppressWarnings("deprecation")
		Job job = new Job(con, "Question2"); 
		job.setJarByClass(Question2.class); 
		job.setMapperClass(BusinessIdMapper.class);
		job.setReducerClass(BusinessIdReducer.class);

		// set output key type 
		job.setOutputKeyClass(Text.class);
		// set output value type 
		job.setOutputValueClass(IntWritable.class);
		//set the HDFS path of the input data
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		// set the HDFS path for the output
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

		//Wait till job completion
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
