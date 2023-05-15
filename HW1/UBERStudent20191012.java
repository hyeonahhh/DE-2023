import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.GenericOptionsParser;
import java.time.DayOfWeek;
import java.time.LocalDate;

public class UBERStudent20191012 
{

	public static class UBERMapper extends Mapper<Object, Text, Text, Text>
	{
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();
		private Text word2 = new Text();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException 
		{
			String[] days = {"MON", "TUE", "WED", "THR", "FRI", "SAT", "SUN"};
			String[] sent = (value.toString()).split(",");
			String region = sent[0];
			String[] dlist = sent[1].split("/");
			String month = dlist[0];
			String day = dlist[1];
			String year = dlist[2];
			LocalDate date = LocalDate.of(Integer.parseInt(year), Integer.parseInt(month), Integer.parseInt(day));
			DayOfWeek dow = date.getDayOfWeek();
			int dowNum = dow.getValue();
			String d = days[dowNum - 1];
			word.set(region + "," + d);
			word2.set(sent[3] + "," + sent[2]);
			context.write(word, word2);
		}
	}

	public static class UBERReducer extends Reducer<Text,Text,Text,Text> 
	{
		Text result = new Text();
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException 
		{
			int tripCount = 0;
			int vehicleCount = 0;
			for (Text val : values) 
			{
				String[] num = (val.toString()).split(",");
				tripCount += Integer.parseInt(num[0]);
				vehicleCount += Integer.parseInt(num[1]);
			}
			result.set(String.valueOf(tripCount) + "," + String.valueOf(vehicleCount));
			context.write(key, result);
		}
	}

	public static void main(String[] args) throws Exception 
	{
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 2) 
		{
			System.err.println("uber");
			System.exit(2);
		}
		Job job = new Job(conf, "uber");
		job.setJarByClass(UBERStudent20191012.class);
		job.setMapperClass(UBERMapper.class);
		job.setCombinerClass(UBERReducer.class);
		job.setReducerClass(UBERReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		FileSystem.get(job.getConfiguration()).delete( new Path(otherArgs[1]), true);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
