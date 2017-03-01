package a3;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class UserReducer<KEY> extends Reducer<KEY, IntWritable, KEY, IntWritable> {
	@Override
	protected void reduce(KEY key, Iterable<IntWritable> values,
			Context context)
			throws IOException, InterruptedException {
	
		int sum = 0;
		Iterator<IntWritable> valuesIt = values.iterator();

		while(valuesIt.hasNext()){
			sum += valuesIt.next().get();
		}
		
		context.write(key, new IntWritable(sum));
	}
}
