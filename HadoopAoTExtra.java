import java.io.IOException;
import java.util.*;
import java.text.SimpleDateFormat;
import java.text.DateFormat;
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

public class HadoopAoTExtra {
	//public static String param;
	//public static String aggregator;
	//public static long start_date;
	//public static long end_date;	
	
	
	// 4 types declared: Type of input key, type of input value, type of output key, type of output value
	public static class MyMapper extends Mapper<Object, Text, Text, DoubleWritable> {
		//private final static LongWritable one = new LongWritable(1);
		
		// The 4 types declared here should match the types that was declared on the top
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			//String parameter;
			Configuration cf = context.getConfiguration();
			String param_arg = cf.get("param");
			String node_id = "node_id";								 					
			String default_parameter = "parameter";
			String line = value.toString();
            		String[] tuple = line.split("\\n");
			double value2;
			String val_hrf = "value_hrf";
			//String param2="temperature";
						
			try{
				for(int i=0; i<tuple.length; i++){
					Object obj2=JSONValue.parse(tuple[i]);
					JSONObject jo = (JSONObject) obj2;
					String key_temp="";
					int temp=0;
					Map<String, String> features = ((Map<String,String>)jo.get("features"));
					String geohash = String.valueOf(jo.get("geohash"));
					String latitude = String.valueOf(jo.get("latitude"));
					String longitude = String.valueOf(jo.get("longitude"));
					String output_str_p2 = ",("+String.valueOf(latitude)+","+String.valueOf(longitude)+"),"+geohash;				
					for (Map.Entry<String, String> entry : features.entrySet()) {
						String k = entry.getKey();
        					String v = String.valueOf(entry.getValue());
						//long time_val = jo.get("timestamp");
					    if (k.equals(default_parameter) && v.equals(param_arg)) {
							//key_temp = v;
							temp=1;
							//context.write(new Text(v), new DoubleWritable(10.00));		
					    }
					    if (k.equals(node_id) && temp==1){
						key_temp = v;
					    }
					    if (k.equals(val_hrf) && temp==1) {
						value2 = Double.parseDouble(v);	
						temp=0;
						String temp_op = key_temp+output_str_p2;
						context.write(new Text(temp_op), new DoubleWritable(value2));

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
			Configuration cf2 = context.getConfiguration();
			Double min_value = Double.parseDouble(cf2.get("min_value"));
			Double max_value = Double.parseDouble(cf2.get("max_value"));		
			String par = cf2.get("param");
			//String cons = "Start Date: " +st_date+" End Date: "+ed_date+" Parameter: "+par+" Aggregate function: "+aggregator+" Value:";
			//double sum = 0;
			String temp_str = "outlier";
			
			for (DoubleWritable tmp: values) {
				//sum+= tmp.get();
				if ((tmp.get() > max_value) || (tmp.get() < min_value)) {
					context.write(key, tmp);	
				}
			}
			//total.set(sum);
			// This write to the final output
			//context.write(new Text(cons), total);
			//context.write(key, total);
		}
	}
	
	
	public static void main(String[] args)  throws Exception {
		Configuration conf = new Configuration();
		conf.set("param", args[1]);
		conf.set("min_value", args[2]);
		conf.set("max_value", args[3]);
		//conf.set("aggregator", args[4]);
		
		Job myjob = Job.getInstance(conf, "my big data outlier removal test");
		myjob.setJarByClass(HadoopAoTExtra.class);
		//adding the following line to make the file run on AWS EC2 - EMR
		mhjob.addFileToCalssPath(new Path("/user/root/json_simple.jar"));
		
		myjob.setMapperClass(MyMapper.class);
		myjob.setReducerClass(MyReducer.class);
		myjob.setOutputKeyClass(Text.class);
		myjob.setOutputValueClass(DoubleWritable.class);
		// Uncomment to set the number of reduce tasks
		// myjob.setNumReduceTasks(2);
		FileInputFormat.addInputPath(myjob, new Path(args[0]));
		FileOutputFormat.setOutputPath(myjob,  new Path(args[4]));

		
		System.exit(myjob.waitForCompletion(true) ? 0 : 1);
	}
	

}
