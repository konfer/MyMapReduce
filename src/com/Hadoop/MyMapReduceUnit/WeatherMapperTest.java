package com.Hadoop.MyMapReduceUnit;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

public class WeatherMapperTest
{
	private Mapper mapper;
	private MapDriver mDriver;

	@Before public void init()
	{
		mapper = new WeatherTest.TempratureMapper();
		mDriver = new MapDriver(mapper);
	}

	@Test public void test() throws IOException
	{
		String testDataLine = "985 07 31 02   200    94 10137   220    26     1     0 -9999";
		mDriver.withInput(new LongWritable(), new Text(testDataLine)).withOutput(new Text("03103"), new IntWritable(200)).runTest();
	}


}
