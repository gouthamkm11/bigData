package solution;

import java.io.IOException;
import java.math.BigInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class turnstileMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
	//public static final Log log_value = LogFactory.getLog(turnstileMapper.class);
	/**
	 * @param args
	 */
	@Override
	 public void map(LongWritable key, Text value, Context context) 
		      throws IOException, InterruptedException {
			  							
		 	String[] input_value = value.toString().split("\n");
		 	
		 	
		 	
		 	for(int i=0;i< input_value.length; i++) {
		 		String[] row = input_value[i].split(",");
		 			String stationName = row[2] + row[3] + row[6] + row[7];
//		 			BigInteger totalEntries = new BigInteger(row[9]);
		 			double totalEntries = 0;
		 			try {
		 				totalEntries = Double.parseDouble(row[9]); 
		 			}
		 			catch(Exception ex) {
		 				totalEntries = 0;
		 			}
//		 			float result = Float.parseFloat(totalEntries);
		 			
		 			context.write(new Text(stationName), new DoubleWritable(totalEntries));
		 	}
		 }
	 }

