package com.bigdata.writable;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TransactionMapper extends Mapper<LongWritable, Text, Text, Transaction> {

	private double totalPrice;

	@Override
	public void map(LongWritable offset, Text record, Context context) 
			throws IOException, InterruptedException {

		String[] recordsplits = record.toString().split(",");
		totalPrice = Double.valueOf(recordsplits[3]) * Integer.valueOf(recordsplits[6]);
		context.write(new Text(recordsplits[4]), new Transaction(totalPrice, 
				Integer.valueOf(recordsplits[6]), Double.valueOf(recordsplits[3]), 1L));
	}
}
