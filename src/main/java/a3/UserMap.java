package a3;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class UserMap extends Mapper<LongWritable, Text, Text, IntWritable>{
	private Text word = new Text();
	private final static IntWritable one = new IntWritable(1);
	
    @Override
    protected void map(LongWritable key, Text value,
			Context context)
			throws IOException, InterruptedException {
		
		String line = value.toString();
		String[] split = line.split(",");
		
		word.set(split[1]);

		context.write(word,one);
		
	}
}
