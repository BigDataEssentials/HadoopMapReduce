package com.bigdata.mapreduce.combiner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class TransactionDriver extends Configured implements Tool {

	public static void main(String[] args) throws Exception {
		int result = ToolRunner.run(new Configuration(), new TransactionDriver(), args);
		System.exit(result);
	}

	@Override
	public int run(String[] args) throws Exception {

		Configuration conf = getConf();
		Job job = Job.getInstance(conf, "Map Reduce without Combiner Demo");

		job.setJarByClass(TransactionDriver.class);
		job.setMapperClass(TransactionMapper.class);
		// job.setCombinerClass(TransactionCombiner.class);
		job.setReducerClass(TransactionReducer.class);
		job.setNumReduceTasks(2);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		return job.waitForCompletion(true) ? 0 : 1;
	}
}
