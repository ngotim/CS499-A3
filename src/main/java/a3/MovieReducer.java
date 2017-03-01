package a3;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MovieReducer<KEY> extends Reducer<KEY, DoubleWritable, KEY, DoubleWritable>{
	@Override
	protected void reduce(KEY key, Iterable<DoubleWritable> values,
			Context context)
			throws IOException, InterruptedException {
	
		double sum = 0;
		Iterator<DoubleWritable> valuesIt = values.iterator();
		int count = 0;
		while(valuesIt.hasNext()){
			sum += valuesIt.next().get();
			count++;
		}
		
		context.write(key, new DoubleWritable(sum/count));
	}
}
