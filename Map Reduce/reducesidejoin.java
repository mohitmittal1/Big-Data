import java.io.IOException;
import java.util.HashMap;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


public class reducesidejoin {
	public static class map1 extends Mapper<LongWritable,Text,Text,Text>
	{
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String dataRecord[] = value.toString().split("\\^");
			String businessId = dataRecord[2];
			int stars = Integer.parseInt(String.valueOf(dataRecord[3].charAt(0)));
			Text businessId1 = new Text (businessId);
			Text count = new Text ("R"+ stars);
			context.write(businessId1, count);
			
		
		}
	}

	public static class map2 extends Mapper<LongWritable,Text,Text,Text>
	{
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
			String dataRecord[] = value.toString().split("\\^");
			String businessId = dataRecord[0];
			Text businessId1 = new Text(businessId);
			Text data = new Text("M" + dataRecord[1] + "\t" + dataRecord[2]);
			context.write(businessId1, data);
			
			
		}
	}
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
		Job job = new Job(con, "reducesidejoin"); 
		job.setJarByClass(reducesidejoin.class); 
		MultipleInputs.addInputPath(job, new Path(otherArgs[0]), TextInputFormat.class, map1.class);
		MultipleInputs.addInputPath(job, new Path(otherArgs[1]), TextInputFormat.class, map2.class);
		job.setReducerClass(JoinReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));

		//Wait till job completion
		job.waitForCompletion(true);
	}
}
