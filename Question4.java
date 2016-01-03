import java.io.IOException;
import java.util.HashMap;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Question4 {
	/***********************************************************************************
	 * The below method formulates logic for the mapper module which gives business id and count as output.
	 * input types:key-longwritable ,value-text
	 * output types:key -text, value-text
	 * Reference: Hadoop Design Guide..Page 65
	 * @see org.apache.hadoop.mapreduce.Mapper#map(KEYIN, VALUEIN, org.apache.hadoop.mapreduce.Mapper.Context)
	 ************************************************************************************/
	public static class Mapper1 extends Mapper<LongWritable, Text, Text, Text>{
		
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] dataRecord = value.toString().split("::");
			if (dataRecord.length > 23) {
				if ("review".compareTo(dataRecord[22]) == 0) {
					String businessID = dataRecord[2];
					int starsAttributeType = Integer.parseInt(dataRecord[20]);
					Text businessId = new Text (businessID);
					//setting a delimiter A..Reference-Hadoop Design Guide
					Text count = new Text ("R" + starsAttributeType);
					context.write(businessId, count);
				}
			}
		}
	}
	/***********************************************************************************
	 * The below method formulates logic for the mapper module which will output business id as key and processed attribute as value.
	 * input types:key-longwritable ,value-text
	 * output types:key -text, value-text
	 *  Reference: Hadoop Design Guide..Page 65
	 * @see org.apache.hadoop.mapreduce.Mapper#map(KEYIN, VALUEIN, org.apache.hadoop.mapreduce.Mapper.Context)
	 ************************************************************************************/

	public static class Mapper2 extends Mapper<LongWritable, Text, Text, Text>{

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] dataRecord = value.toString().split("::");
			if (dataRecord.length > 23) {
				String businessID = dataRecord[2];
				Text businessId = new Text (businessID);
				//setting a delimiter B|
				Text attributeValue = new Text ("M#" + dataRecord[3] + "\t" + dataRecord[10]);
				context.write(businessId, attributeValue);
			}
		}
	}
	/***********************************************************************************
	 * The below class formulates the reducer logic for joining. 
	 * input types: key-text ,value-text
	 * output types: key -text, value-text
	 ************************************************************************************/

	public static class JoinReducer extends Reducer<Text, Text, Text, Text> {
		class CompositeKeyCreator {
		    double average = 0.0;
		    String attributeAddressCategoryValue = "";
		    //setters and getters for average and address ,category attributes.
			public double getAverage() {
				return average;
			}

			public void setAverage(double averageValue) {
				this.average = averageValue;
			}

			public String getAddressCategoryAttributeValue() {
				return attributeAddressCategoryValue;
			}

			public void setAddressCategoryAttributeValue(String addressCategoryValue) {
				this.attributeAddressCategoryValue = addressCategoryValue;
			}

			

		    public CompositeKeyCreator(double averageValue, String addressCategoryValue) {
		        this.average = averageValue;
		        this.attributeAddressCategoryValue = addressCategoryValue;
		    }

		    public String toString() {
		        return (this.average + "\t" + this.attributeAddressCategoryValue);
		    }
		}
		
		private java.util.Map<String, CompositeKeyCreator> hasMapInstanceForTopTenRecords = new HashMap<String, CompositeKeyCreator>(10);
		
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			int count = 0; 
			int numberOfEntries = 0;
			double average = 0.0;
			String attributeForAddressCaetogryValue = "";
			boolean flag = false;
			//traversing to number of entries.
			for (Text val : values) {
				if (val.toString().charAt(0) == 'R') {
					count += Integer.parseInt(val.toString().substring(1));
					numberOfEntries++;
				}
				//processing the address and category attribute values.
				else if (val.toString().charAt(0) == 'M') {
					String processedAttributeValue = val.toString().substring(2);
					flag = true;
					attributeForAddressCaetogryValue = processedAttributeValue;
				}
				
			}
			
			if (flag) {
				average = (double)Math.round(((count * 1.0) / numberOfEntries) * 100) / 100;
				CompositeKeyCreator compKey = new CompositeKeyCreator(average, attributeForAddressCaetogryValue);
				if (this.hasMapInstanceForTopTenRecords.size() < 10) {
					hasMapInstanceForTopTenRecords.put(key.toString(), compKey);
		        } 
				else {
		            for (Entry<String, CompositeKeyCreator> e : this.hasMapInstanceForTopTenRecords.entrySet()) {
		                if (average > e.getValue().getAverage()) {
		                    this.hasMapInstanceForTopTenRecords.remove(e.getKey());
		                    this.hasMapInstanceForTopTenRecords.put(key.toString(), compKey);
		                    break;
		                }
		            }
		        }
			}
		}
		
		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			for (Entry<String, CompositeKeyCreator> e : this.hasMapInstanceForTopTenRecords.entrySet()) {
                context.write(new Text(e.getKey()), new Text (e.getValue().toString()));
            }
	    }
	}
	
	/**********************************************************************************
	 * The main method formulates driver logic for reduce side join. 
	 * @param args
	 * @throws Exception
	 **********************************************************************************/
	public static void main(String[] args) throws Exception {
		Configuration con = new Configuration();
		String[] otherArgs = new GenericOptionsParser(con, args).getRemainingArgs();
		
		if (otherArgs.length != 3) {
			System.err.println("Command line arguments: Question4 #Input File 1#  #Input File 2# #Output File Directory#");
			System.exit(2);
		}

		@SuppressWarnings("deprecation")
		Job job = new Job(con, "Question4"); 
		job.setJarByClass(Question4.class); 
		MultipleInputs.addInputPath(job, new Path(otherArgs[0]), TextInputFormat.class, Mapper1.class);
		MultipleInputs.addInputPath(job, new Path(otherArgs[1]), TextInputFormat.class, Mapper2.class);
		job.setReducerClass(JoinReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));

		//Wait till job completion
		job.waitForCompletion(true);
	}
}
