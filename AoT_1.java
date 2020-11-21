
import java.io.IOException;
import java.util.*;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator; 
import java.util.Map; 
//import org.json.*;
import org.json.simple.JSONArray; 
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

public class AoT_1 {
	
	// 4 types declared: Type of input key, type of input value, type of output key, type of output value
	public static class MyMapper extends Mapper<Object, Text, Text, LongWritable> {
		private final static LongWritable one = new LongWritable(1);
		
		// The 4 types declared here should match the types that was declared on the top
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			//String features;
			String parameter;
			String default_parameter = "parameter";
			String line = value.toString();
            		String[] tuple = line.split("\\n");
			try{
				for(int i=0; i<tuple.length; i++){
					Object obj2=JSONValue.parse(tuple[i]);
					JSONObject jo = (JSONObject) obj2;

					Map<String, String> features = ((Map)jo.get("features"));
					// iterating address Map 
					Iterator<Map.Entry> itr1 = features.entrySet().iterator(); 
					while (itr1.hasNext()) { 
					    Map.Entry<String,String> pair = itr1.next(); 
					    //System.out.println(pair.getKey() + " : " + pair.getValue());
					    if (pair.containsKey(default_parameter)) {
					    	context.write(new Text(pair.getValue()), one);
					    }
					} 
					//features = obj.getString("features");
					//JSONObject obj_inner = new JSONObject(features);
					// parameter = obj_inner.getString("parameter");
					
				}
			} catch (Exception e){
				e.printStackTrace();
			}
		}
		
	}
	
	

	// 4 types declared: Type of input key, type of input value, type of output key, type of output value
	// The input types of reduce should match the output type of map
	public static class MyReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
		private LongWritable total = new LongWritable();
		
		// Notice the that 2nd argument: type of the input value is an Iterable collection of objects 
		//  with the same type declared above/as the type of output value from map
		public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
			long sum = 0;
			for (LongWritable tmp: values) {
				sum += tmp.get();
			}
			total.set(sum);
			// This write to the final output
			context.write(key, total);
		}
	}
	
	
	public static void main(String[] args)  throws Exception {
		Configuration conf = new Configuration();
		Job myjob = Job.getInstance(conf, "my word count test");
		myjob.setJarByClass(AoT_1.class);
		myjob.setMapperClass(MyMapper.class);
		myjob.setReducerClass(MyReducer.class);
		myjob.setOutputKeyClass(Text.class);
		myjob.setOutputValueClass(LongWritable.class);
		// Uncomment to set the number of reduce tasks
		// myjob.setNumReduceTasks(2);
		FileInputFormat.addInputPath(myjob, new Path(args[0]));
		FileOutputFormat.setOutputPath(myjob,  new Path(args[1]));
		System.exit(myjob.waitForCompletion(true) ? 0 : 1);
	}
	

}
