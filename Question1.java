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

public class Question1 {
	/**********************************************************************************
	 * The below class formulates logic for the mapper/reducer module to count words.
	 **********************************************************************************/
	public static class WordCountMapper	extends Mapper<LongWritable, Text, Text, IntWritable>{

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
			//reading all the attributes/column names ..split by :: (delimiter)
			String[] dataRecord = value.toString().split("::");
			//checking if there are 24 columns(attributes) in the data set or not.
			if (dataRecord.length > 23) {
				//checking for the type attribute in each of the entity.
				//dataRecord[22] specifies the "type" attribute.
				//comparing each entity type(review/business/user)
				//value with the value of type attribute which is the 23rd attribute.
				if ("review".compareTo(dataRecord[22]) == 0
						|| "business".compareTo(dataRecord[22]) == 0
						|| "user".compareTo(dataRecord[22]) == 0) {
                     Text entity =new Text(dataRecord[22]);
                     //returning the key(entity type) with value one for each record.
					context.write(entity, one);
				}
			}
		}
	}

/***********************************************************************************
 * The below class formulates the reducer logic. 
 * input types: key-text ,value-intwritable
 * output types: key -text, value-intwritable
 ************************************************************************************/
	public static class WordCountReducer
	extends Reducer<Text,IntWritable,Text,IntWritable> {

		private IntWritable outputValue= new IntWritable();
		public void reduce(Text key, Iterable<IntWritable> values,
				Context context
				) throws IOException, InterruptedException {
			int countOfEntities = 0; // initial value for count
			//iterating over all the values for a particular entity type and recording the count.
			for (IntWritable entityOccurance : values) {
				countOfEntities += entityOccurance.get();
			}
			outputValue.set(countOfEntities);
			context.write(key, outputValue); 
		}
	}
	/**********************************************************************************
	 * The main method formulates logic for the word count driver setup.
	 * @param args
	 * @throws Exception
	 **********************************************************************************/
	public static void main(String[] args) throws Exception {
		Configuration con = new Configuration();
		String[] otherArgs = new GenericOptionsParser(con, args).getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Command line arguments: Question1 #Input File# #Output File Directory#");
			System.exit(2);
		}

		@SuppressWarnings("deprecation")
		Job job = new Job(con, "Question1"); 
		job.setJarByClass(Question1.class); 
		job.setMapperClass(WordCountMapper.class);
		job.setReducerClass(WordCountReducer.class);

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
