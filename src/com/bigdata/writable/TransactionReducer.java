package com.bigdata.writable;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TransactionReducer extends Reducer<Text, Transaction, Text, Transaction> {

	@Override
	public void reduce(Text paymentType, Iterable<Transaction> transactions, Context context) 
			throws IOException, InterruptedException {
		double sumPrice = 0d;
		int minQuantity = Integer.MAX_VALUE;
		double maxUnitPrice = Double.MIN_VALUE;
		long count = 0;
		for (Transaction record : transactions) {			
			sumPrice += record.getSumPrice();
			minQuantity = Math.min(minQuantity, record.getMinQuantity());
			maxUnitPrice = Math.max(maxUnitPrice, record.getMaxUnitPrice());
			count += Long.valueOf(record.getCount());
		}
		context.write(paymentType, new Transaction(sumPrice, minQuantity, maxUnitPrice, count));
	}
}
