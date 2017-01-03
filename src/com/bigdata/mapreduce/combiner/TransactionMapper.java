package com.bigdata.mapreduce.combiner;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TransactionMapper extends Mapper<LongWritable, Text, Text, Text> {

	private double totalPrice;
	private String output;

	@Override
	public void map(LongWritable offset, Text record, Context context) 
			throws IOException, InterruptedException {

		String[] recordsplits = record.toString().split(",");
		totalPrice = Double.valueOf(recordsplits[3]) * Integer.valueOf(recordsplits[6]);
		output = totalPrice + "|" + recordsplits[6] + "|" + recordsplits[3] + "|1";
		context.write(new Text(recordsplits[4]), new Text(output));
	}
}
