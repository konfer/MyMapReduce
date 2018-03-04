package com.Hadoop.MyMapReduceUnit;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.junit.Before;
import org.junit.Test;

public class WeatherMapreduceUnitTest
{
	private Mapper weatherMapper;
	private Reducer weatherReducer;
	private MapReduceDriver mrDriver;

	@Before
	public void init()
	{
		weatherMapper= new WeatherTest.TempratureMapper();
		weatherReducer=new WeatherTest.TempratureReduce();
		mrDriver=new MapReduceDriver(weatherMapper,weatherReducer);
	}

	@Test
	public void test()
	{
		String line = "1985 07 31 02   200    94 10137   220    26     1     0 -9999";
		String line2 = "1985 07 31 11   100    56 -9999    50     5 -9999     0 -9999";

		mrDriver.withInput(new LongWritable(),new Text(line)).withInput(new LongWritable(),new Text(line2))
				.withOutput(new Text("03103"),new IntWritable(150)).runTest();
	}



}

