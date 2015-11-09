import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class topTen {
	public static class TopTenMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
	
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] dataRecord = value.toString().split("\\^");
					int starsAttributeType = Integer.parseInt(String.valueOf(dataRecord[3].charAt(0)));
					Text outputKey=new Text(dataRecord[2]);//mapper output key
					IntWritable outputValue = new IntWritable (starsAttributeType);//mapper output value
					context.write(outputKey, outputValue);
				}
			}
			
		
	
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
	            for (java.util.Map.Entry<String, Double> e : this.hasMapInstanceForTopTenRecords.entrySet()) {
	                if (averageRating > e.getValue()) {
	                    this.hasMapInstanceForTopTenRecords.remove(e.getKey());//remove if there is a record with lesser average
	                    this.hasMapInstanceForTopTenRecords.put(key.toString(), averageRating);
	                    break;
	                }
	            }
	        }
		}
		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			for (java.util.Map.Entry<String, Double> e : this.hasMapInstanceForTopTenRecords.entrySet()) {
                context.write(new Text(e.getKey()), NullWritable.get());
            }
	    }
	}
	
	public static void main(String[] args) throws Exception {
		Configuration con = new Configuration();
		String[] otherArgs = new GenericOptionsParser(con, args).getRemainingArgs();
		
		if (otherArgs.length != 2) {
			System.err.println("Command line argumenst: topTen #Input File# #Output File# ");
			System.exit(2);
		}

		
		@SuppressWarnings("deprecation")
		Job job = new Job(con, "topTen"); 
		job.setJarByClass(topTen.class); 
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


