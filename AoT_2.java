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

public class AoT_2 {
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
			String st_date_arg = cf.get("start_date");
			String ed_date_arg = cf.get("end_date");
			SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
			
			//Date start_date_arg = sdf.parse(st_date_arg);
			//Date end_date_arg = sdf.parse(ed_date_arg);
			
			
			
			String default_parameter = "parameter";
			String line = value.toString();
            		String[] tuple = line.split("\\n");
			double value2;
			String val_hrf = "value_hrf";
			//String param2="temperature";
			 
			
			try{
				Date start_date_arg = sdf.parse(st_date_arg);
				Date end_date_arg = sdf.parse(ed_date_arg);
				for(int i=0; i<tuple.length; i++){
					Object obj2=JSONValue.parse(tuple[i]);
					JSONObject jo = (JSONObject) obj2;
					String key_temp="";
					int temp=0;
					Map<String, String> features = ((Map<String,String>)jo.get("features"));
					String t_val = String.valueOf(jo.get("timestamp"));
					Date time_val = new Date(Long.parseLong(t_val));
					for (Map.Entry<String, String> entry : features.entrySet()) {
						String k = entry.getKey();
        					String v = String.valueOf(entry.getValue());
						//long time_val = jo.get("timestamp");
					    if (k.equals(default_parameter) && v.equals(param_arg) && time_val.after(start_date_arg) && time_val.before(end_date_arg)) {
							key_temp = v;
							temp=1;
							//context.write(new Text(v), new DoubleWritable(10.00));		
					    }
					    if (k.equals(val_hrf) && temp==1) {
						value2 = Double.parseDouble(v);	
						temp=0;
						context.write(new Text(key_temp), new DoubleWritable(value2));

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
			String aggregator = cf2.get("aggregator");
			String st_date = cf2.get("start_date");
			String ed_date = cf2.get("end_date");
			String par = cf2.get("param");
			String cons = "Start Date: " +st_date+" End Date: "+ed_date+" Parameter: "+par+" Aggregate function: "+aggregator+" Value:";
			double sum = 0;
			int counter = 0;
			double max_val = Double.MIN_VALUE;
			double min_val = Double.MAX_VALUE;
			String aggregator2 = "avg";
			if(aggregator == "max"){
				for (DoubleWritable tmp: values) {
					//sum += tmp.get();
					//counter++;
					if(tmp.get() > max_val){
						max_val = tmp.get();
					}
				}
				total.set(max_val);
			}
			else if(aggregator == "min"){
				for (DoubleWritable tmp: values) {
					//sum += tmp.get();
					//counter++;
					if(tmp.get() < min_val){
						min_val = tmp.get();
					}
				}
				total.set(min_val);				
			}
			else if(aggregator == "avg") {
				for (DoubleWritable tmp: values) {
					sum += tmp.get();
					counter++;
				}
				try{
					total.set(sum/counter);
				} catch(NullPointerException ne){
					ne.printStackTrace();
				}
			}
			
			//for (DoubleWritable tmp: values) {
			//	sum+= tmp.get();
			//}
			//total.set(sum);
			// This write to the final output
			context.write(new Text(cons), total);
			//context.write(new Text(aggregator), new DoubleWritable(sum));
		}
	}
	
	
	public static void main(String[] args)  throws Exception {
		Date start_date_ = new SimpleDateFormat("yyyy-MM-dd").parse(args[1]);
		//start_date = start_date_.getTime();
		Date end_date_ = new SimpleDateFormat("yyyy-MM-dd").parse(args[2]);
		//end_date = end_date_.getTime();
		SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");	
		String st_date = df.format(start_date_);
		String ed_date = df.format(end_date_);
		//param = args[3];
		//aggregator = args[4];
		Configuration conf = new Configuration();
		conf.set("start_date", st_date);
		conf.set("end_date", ed_date);
		conf.set("param", args[3]);
		conf.set("aggregator", args[4]);
		Job myjob = Job.getInstance(conf, "my specific parameter specific aggregate test");
		myjob.setJarByClass(AoT_2.class);
		myjob.setMapperClass(MyMapper.class);
		myjob.setReducerClass(MyReducer.class);
		myjob.setOutputKeyClass(Text.class);
		myjob.setOutputValueClass(DoubleWritable.class);
		// Uncomment to set the number of reduce tasks
		// myjob.setNumReduceTasks(2);
		FileInputFormat.addInputPath(myjob, new Path(args[0]));
		FileOutputFormat.setOutputPath(myjob,  new Path(args[5]));

		
		System.exit(myjob.waitForCompletion(true) ? 0 : 1);
	}
	

}
