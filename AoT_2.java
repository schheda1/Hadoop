import java.io.IOException;
import java.util.*;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator; 
import java.util.Map; 
//import org.json.*;

import org.json.simple.JSONObject; 
import org.json.simple.parser.*;
import org.json.simple.JSONValue;
import org.json.simple.*;

import org.apache.commons.lang.WordUtils;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;

public class AoT_2 {
	public static String param;
	public static String aggregator;
	public static Date start_date;
	public static Date end_date;	
	
	
	// 4 types declared: Type of input key, type of input value, type of output key, type of output value
	public static class MyMapper extends Mapper<Object, Text, Text, DoubleWritable> {
		//private final static LongWritable one = new LongWritable(1);
		
		// The 4 types declared here should match the types that was declared on the top
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			//String parameter;
			//String default_parameter = "parameter";
			String line = value.toString();
            String[] tuple = line.split("\\n");
			String value;
			//double store_data[] = new store_data[tuple.length]; 
			//int data_idx=0;
			try{
				for(int i=0; i<tuple.length; i++){
					Object obj2=JSONValue.parse(tuple[i]);
					JSONObject jo = (JSONObject) obj2;

					Map<String, String> features = ((Map<String,String>)jo.get("features"));
					for (Map.Entry<String, String> entry : features.entrySet()) {
						String k = entry.getKey();
        				String v = entry.getValue();
						long time_val = Long.parseLong(features.get("timestamp"));
					    if (k.equals(param)) {
					    	//context.write(new Text(v), one);
							value = features.get("value_hrf");
							//store_data[data_idx] = Double.parseDouble(value);
							//data_idx++;
							context.write(new Text(v), new DoubleWritable(value));
							
					    }
					}
				}
			} catch (Exception e){
				e.printStackTrace();
			}
		}
		
	}
	
	

	// 4 types declared: Type of input key, type of input value, type of output key, type of output value
	// The input types of reduce should match the output type of map
	public static class MyReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
		//private LongWritable total = new LongWritable();
		private DoubleWritable total = new DoubleWritable();
		
		// Notice the that 2nd argument: type of the input value is an Iterable collection of objects 
		//  with the same type declared above/as the type of output value from map
		public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
			double sum = 0;
			int counter = 0;
			double max_val = Double.MIN_VALUE;
			double min_val = Double.MAX_VALUE;
			
			if(aggregator.equals("max")){
				for (DoubleWritable tmp: values) {
					//sum += tmp.get();
					//counter++;
					if(tmp.get() > max_val){
						max_val = tmp.get();
					}
				}
				total.set(max_val);
			}
			else if(aggregator.equals("min")){
				for (DoubleWritable tmp: values) {
					//sum += tmp.get();
					//counter++;
					if(tmp.get() < min_val){
						min_val = tmp.get();
					}
				}
				total.set(min_val);				
			}
			else {
				for (DoubleWritable tmp: values) {
					sum += tmp.get();
					counter++;
				}
				total.set(sum/counter);
			}

			// This write to the final output
			context.write(key, total);
		}
	}
	
	
	public static void main(String[] args)  throws Exception {
		start_date = new SimpleDateFormat("yyyy-mm-dd").parse(args[1]);
		//start_date = start_date.getTime();
		end_date = new SimpleDateFormat("yyyy-mm-dd").parse(args[2]);
		//end_date = end_date.getTime();
		param = args[3];
		aggregator = args[4];
		Configuration conf = new Configuration();
		Job myjob = Job.getInstance(conf, "my specific parameter specific aggregate test");
		myjob.setJarByClass(AoT_1.class);
		myjob.setMapperClass(MyMapper.class);
		myjob.setReducerClass(MyReducer.class);
		myjob.setOutputKeyClass(Text.class);
		myjob.setOutputValueClass(LongWritable.class);
		// Uncomment to set the number of reduce tasks
		// myjob.setNumReduceTasks(2);
		FileInputFormat.addInputPath(myjob, new Path(args[0]));
		FileOutputFormat.setOutputPath(myjob,  new Path(args[5]));

		
		System.exit(myjob.waitForCompletion(true) ? 0 : 1);
	}
	

}
