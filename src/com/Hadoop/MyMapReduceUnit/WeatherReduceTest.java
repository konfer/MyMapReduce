package com.Hadoop.MyMapReduceUnit;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class WeatherReduceTest
{
	private Reducer weatherReduceTest;
	private ReduceDriver reduceDriver;

	@Before public void init()
	{
		weatherReduceTest = new WeatherTest.TempratureReduce();
		reduceDriver = new ReduceDriver(weatherReduceTest);
	}

	@Test
	public void test()
	{
		String key = "03103";
		List values = new ArrayList();
		values.add(new IntWritable(200));
		values.add(new IntWritable(100));

		reduceDriver.withInput(new Text(key), values).withOutput(new Text(key), new IntWritable(150)).runTest();
	}


}
