import java.io.IOException;

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
public class BussinessId {
	
	public static class BusinessIdMapper extends Mapper <LongWritable, Text, Text, IntWritable>{
		private final static IntWritable one = new IntWritable(1);
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
			String[] datarecord = value.toString().split("\\^");
		//	System.err.println("datarecord is ::"+datarecord);
			if(datarecord[1].contains("Palo Alto") == true ){
				
				Text outputKey = new Text(datarecord[0]);
				context.write(outputKey,one);
			//	System.out.print(datarecord[0]);
				
			}
		}
	}
	public static class BusinessIdReducer extends Reducer<Text,IntWritable,Text,NullWritable> {
		private NullWritable outputValue = NullWritable.get();

		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			//outputting business id as key and null as value.
			context.write(key, outputValue); 
		}
	}

public static void main(String[] args) throws Exception {
	Configuration con = new Configuration();
	String[] otherArgs = new GenericOptionsParser(con, args).getRemainingArgs();
	
	if (otherArgs.length != 2) {
		System.err.println("Command line arguments: BussinessId #Input File# #Output File#>");
		System.exit(2);
	}

	
	@SuppressWarnings("deprecation")
	Job job = new Job(con, "BusinessId"); 
	job.setJarByClass(BussinessId.class); 
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

