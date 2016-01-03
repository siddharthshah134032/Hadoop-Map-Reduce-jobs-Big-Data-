import java.io.IOException;
import java.util.HashMap;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Question3 {
	/**********************************************************************************
	 * The below class formulates logic for the mapper/reducer module to find the top ten 
	 * rated businesses using the average ratings. 
	 **********************************************************************************/
	public static class TopTenMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
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
			if (dataRecord.length > 23) {
				if ("review".compareTo(dataRecord[22]) == 0) {
					//if entity type is "review" make the count of stars.
					int starsAttributeType = Integer.parseInt(dataRecord[20]);
					Text outputKey=new Text(dataRecord[2]);//mapper output key
					IntWritable outputValue = new IntWritable (starsAttributeType);//mapper output value
					context.write(outputKey, outputValue);
				}
			}
			
		}
	}
	/***********************************************************************************
	 * The below class formulates the reducer logic. 
	 * input types: key-text ,value-intwritable
	 * output types: key -text, value-nullwritable
	 ************************************************************************************/
	public static class TopTenReducer extends Reducer<Text, IntWritable, Text, NullWritable> {
		private java.util.Map<String, Double> hasMapInstanceForTopTenRecords = new HashMap<String, Double>(10);
		
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			int count = 0; 
			int numberOfEntries = 0;
			for (IntWritable val : values) {
				count += val.get();
				numberOfEntries++;
			}
			
			double averageRating = (double)Math.round(((count * 1.0) / numberOfEntries) * 100) / 100;
			
			if (this.hasMapInstanceForTopTenRecords.size() < 10) {
				hasMapInstanceForTopTenRecords.put(key.toString(), averageRating);
	        } 
			else {
	            for (Entry<String, Double> e : this.hasMapInstanceForTopTenRecords.entrySet()) {
	                if (averageRating > e.getValue()) {
	                    this.hasMapInstanceForTopTenRecords.remove(e.getKey());//remove if there is a record with lesser average
	                    this.hasMapInstanceForTopTenRecords.put(key.toString(), averageRating);
	                    break;
	                }
	            }
	        }
		}
		/***************************************************************************
		 * Reference- Hadoop Design Guide
		 * (non-Javadoc)
		 * @see org.apache.hadoop.mapreduce.Reducer#cleanup(org.apache.hadoop.mapreduce.Reducer.Context)
		 *******************************************************************************/
		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			for (Entry<String, Double> e : this.hasMapInstanceForTopTenRecords.entrySet()) {
                context.write(new Text(e.getKey()), NullWritable.get());
            }
	    }
	}
	
	/**********************************************************************************
	 * The main method formulates logic for driver  the top ten rated businesses using the average ratings. 
	 * @param args
	 * @throws Exception
	 **********************************************************************************/
	public static void main(String[] args) throws Exception {
		Configuration con = new Configuration();
		String[] otherArgs = new GenericOptionsParser(con, args).getRemainingArgs();
		
		if (otherArgs.length != 2) {
			System.err.println("Command line argumenst: Question3 #Input File# #Output File# ");
			System.exit(2);
		}

		
		@SuppressWarnings("deprecation")
		Job job = new Job(con, "Question3"); 
		job.setJarByClass(Question3.class); 
		job.setMapperClass(TopTenMapper.class);
		job.setReducerClass(TopTenReducer.class);
		

		job.setOutputKeyClass(Text.class);
		// set output value type 
		job.setOutputValueClass(IntWritable.class);
		//set the HDFS path of the input data
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		// set the HDFS path for the output
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

		//Wait till job completion
		job.waitForCompletion(true);
	}
}
