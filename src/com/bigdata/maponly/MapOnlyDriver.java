package com.bigdata.maponly;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class MapOnlyDriver extends Configured implements Tool {

	public static void main(String[] args) throws Exception {
		int result = ToolRunner.run(new Configuration(), 
				new MapOnlyDriver(), args);
		System.exit(result);
	}

	public int run(String[] args) throws Exception {

		Configuration conf = getConf();
		Job job = Job.getInstance(conf, "Map Only Job Demo");

		job.setJarByClass(MapOnlyDriver.class);
		job.setMapperClass(MapOnlyMapper.class);
		job.setNumReduceTasks(0);
		
		job.setMapOutputKeyClass(NullWritable.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
				
		return job.waitForCompletion(true) ? 0 : 1;
	}

}
