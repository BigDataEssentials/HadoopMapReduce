package com.bigdata.writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class Transaction implements Writable {

	private double sumPrice;
	private int minQuantity;
	private double maxUnitPrice;
	private long count;
	
	public Transaction() {
		this.sumPrice = 0d;
		this.minQuantity = 0;
		this.maxUnitPrice = 0d;
		this.count = 0l;
	}
	
	public Transaction(double sumPrice, int minQuantity, 
			double maxUnitPrice, long count) {
		this.sumPrice = sumPrice;
		this.minQuantity = minQuantity;
		this.maxUnitPrice = maxUnitPrice;
		this.count = count;
	}
	
	public double getSumPrice() {
		return sumPrice;
	}

	public int getMinQuantity() {
		return minQuantity;
	}

	public double getMaxUnitPrice() {
		return maxUnitPrice;
	}

	public long getCount() {
		return count;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		sumPrice = in.readDouble();
		minQuantity = in.readInt();
		maxUnitPrice = in.readDouble();
		count = in.readLong();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeDouble(sumPrice);
		out.writeInt(minQuantity);
		out.writeDouble(maxUnitPrice);
		out.writeLong(count);		
	}

	@Override
	public String toString() {
		return sumPrice + "|" + minQuantity + "|" + maxUnitPrice + "|" + count;
	}

}
