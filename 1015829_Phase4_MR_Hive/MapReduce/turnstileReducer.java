package solution;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;



	/**
	 * @param args
	 */
	
	public class turnstileReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
		
		@Override
		public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
				throws IOException, InterruptedException {
				long totalEntries = 0;
				int count = 0;
				long averageEntry = 0;
			
				/*
				 * For each value in the set of values passed to us by the mapper:
				 */
				for (DoubleWritable value : values) {
					
					count++;
				  /*
				   * Add the value to the word count counter for this key.
				   */
					totalEntries += value.get();
				}
				
				averageEntry = totalEntries/count;
				
				/*
				 * Call the write method on the Context object to emit a key
				 * and a value from the reduce method. 
				 */
				context.write(key, new DoubleWritable(averageEntry));
		}
	}


