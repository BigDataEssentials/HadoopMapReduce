package com.bigdata.mapreduce.reducerascombiner;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TransactionReducer extends Reducer<Text, Text, Text, Text> {

	private String output;

	@Override
	public void reduce(Text paymentType, Iterable<Text> records, Context context) 
			throws IOException, InterruptedException {
		double sumPrice = 0d;
		int minQuantity = Integer.MAX_VALUE;
		double maxUnitPrice = Double.MIN_VALUE;
		long count = 0;
		for (Text record : records) {
			String[] recordSplit = record.toString().split("\\|");
			sumPrice += Double.valueOf(recordSplit[0]);
			minQuantity = Math.min(minQuantity, Integer.valueOf(recordSplit[1]));
			maxUnitPrice = Math.max(maxUnitPrice, Double.valueOf(recordSplit[2]));
			count += Long.valueOf(recordSplit[3]);
		}
		output = sumPrice + "|" + minQuantity + "|" + maxUnitPrice + "|" + count;
		context.write(paymentType, new Text(output));
	}
}
