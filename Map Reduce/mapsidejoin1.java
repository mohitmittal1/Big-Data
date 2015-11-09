import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.PriorityQueue;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class mapsidejoin2 {
	public static class Map1 extends Mapper<LongWritable, Text, Text, DoubleWritable> {
		ArrayList<String> myCenterList;

		@Override
		public void setup(Context context) throws IOException, InterruptedException {
			super.setup(context);
			myCenterList = new ArrayList<String>();
			Configuration conf = context.getConfiguration();
			String myfilepath = conf.get("myfilepath");
			Path part = new Path("hdfs://cshadoop1" + myfilepath);// Location
																					// of
			// file in
			// HDFS

			FileSystem fs = FileSystem.get(conf);
			FileStatus[] fss = fs.listStatus(part);
			for (FileStatus status : fss) {
				Path pt = status.getPath();

				BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(pt)));
				String line;
				line = br.readLine();
				while (line != null) {
					String[] record = line.toString().split("\\^");
					if (record.length > 1 && record[1].contains("Stanford")) {
						myCenterList.add(record[0]);
					}
					line = br.readLine();
				}

			}
		}

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] record = value.toString().split("\\^");
			if (myCenterList.contains(record[2].toString())) {
				context.write(new Text(record[1]), new DoubleWritable(Double.parseDouble(record[3])));
			}
		}
	}

	public static void main(String args[]) throws Exception {
		Configuration conf = new Configuration();
		 conf.set("myfilepath",args[1]);

		Job job = new Job(conf, "Stanford");
		job.setJarByClass(mapsidejoin2.class);
		job.setMapperClass(Map1.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[2]));
		job.waitForCompletion(true);

	}
}
