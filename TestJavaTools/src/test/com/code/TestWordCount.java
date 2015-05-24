package test.com.code;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.MapDriver;
import org.apache.hadoop.mrunit.MapReduceDriver;
import org.apache.hadoop.mrunit.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import com.code.WordCount.Map;
import com.code.WordCount.Reduce;

public class TestWordCount {
	MapDriver<LongWritable, Text, Text, IntWritable> mapDriver;
	ReduceDriver<Text, IntWritable, Text, IntWritable> reduceDriver;
	MapReduceDriver<LongWritable, Text, Text, IntWritable, Text, IntWritable> mapReduceDriver;
	
	@Before
	public void setUp(){
		Map mapper = new Map();
		Reduce reducer = new Reduce();
		mapDriver = MapDriver.newMapDriver(mapper);
		reduceDriver = ReduceDriver.newReduceDriver(reducer);
		mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
	}
	
	@Test
	public void testMapper() throws Exception{
		mapDriver.withInput(new LongWritable(), new Text("world,abc"));
		mapDriver.withOutput(new Text("world"), new IntWritable(1));
		mapDriver.withOutput(new Text("abc"), new IntWritable(1));
		try{
			mapDriver.runTest();
		}catch(Exception ex){
			ex.printStackTrace();
		}
	}
	
	@Test
	public void testReducer() throws Exception{
		List<IntWritable> values = new ArrayList<IntWritable>();
		values.add(new IntWritable(1));
		values.add(new IntWritable(1));
		reduceDriver.withInput(new Text("world"),values);
		reduceDriver.withOutput(new Text("world"), new IntWritable(2));
		try{
			reduceDriver.runTest();
		}catch(Exception ex){
			ex.printStackTrace();
		}
	}
}
