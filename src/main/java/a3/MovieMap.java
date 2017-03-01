package a3;
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MovieMap extends Mapper<LongWritable, Text, Text, DoubleWritable>{
	private Text word = new Text();
	private DoubleWritable rate = new DoubleWritable();
	
    @Override
    protected void map(LongWritable key, Text value,
			Context context)
			throws IOException, InterruptedException {
		
		String line = value.toString();
		String[] split = line.split(",");
		
		word.set(split[0]);
		//System.out.println(split[0] + " " + split[2]);
		rate.set(Double.parseDouble(split[2]));
		context.write(word,rate);
		
	}
}
