package com.bigdata.maponly;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MapOnlyMapper extends
		Mapper<LongWritable, Text, NullWritable, Text> {

	private final String outDelimiter = "|";
	private double totalPrice;

	@Override
	public void map(LongWritable offset, Text record, Context context)
			throws IOException, InterruptedException {

		String[] recordsplits = record.toString().split(",");
		String[] customersplits = recordsplits[7].split("~");
		String[] suppliersplits = recordsplits[8].split("~");

		totalPrice = Double.valueOf(recordsplits[3])
				* Integer.valueOf(recordsplits[6]);
		StringBuilder sb = new StringBuilder(recordsplits[0])
				.append(outDelimiter);
		sb.append(recordsplits[1]).append(outDelimiter);
		sb.append(recordsplits[2]).append(outDelimiter);
		sb.append(totalPrice).append(outDelimiter);
		sb.append(recordsplits[4]).append(outDelimiter);
		sb.append(recordsplits[5]).append(outDelimiter);
		sb.append(customersplits[0]).append(outDelimiter);
		sb.append(customersplits[1] + " " + customersplits[2]).append(
				outDelimiter);
		sb.append(customersplits[3]).append(outDelimiter);
		sb.append(suppliersplits[0]).append(outDelimiter);
		sb.append(suppliersplits[2]);

		context.write(NullWritable.get(), new Text(sb.toString()));
	}
}
