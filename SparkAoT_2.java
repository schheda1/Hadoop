package assignment5;

import java.util.Arrays;
import java.util.List;
import java.util.ArrayList;
import java.lang.Iterable;
import java.util.Map;
import java.util.Iterator;
import java.util.Date;
import java.text.SimpleDateFormat;
/* Spark imports */
import org.json.simple.*;
import org.json.simple.parser.*;

import scala.Tuple2;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.PairFlatMapFunction;
public class SparkAoT_2 {

    
    /**
     * args[0]: Input file path on distributed file system
     * args[1]: Start Date yyyy-mm-dd
     * args[2]: End Date yyyy-mm-dd
     * args[3]: Parameter name
     * args[4]: Aggregate Function (min, max, avg)
     * args[5]: Output file path on distributed file system
     */
	public static int counter =0;

    public static void main(String[] args) throws Exception {
	System.out.println("Hello World from Java");

	String input = args[0];
	Date start_date = new SimpleDateFormat("yyyy-MM-dd").parse(args[1]);
	//long start_date_new = start_date.getTime();
	Date end_date = new SimpleDateFormat("yyyy-MM-dd").parse(args[2]);
	//long end_date_new = end_date.getTime();
	SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");
	String st_dt = df.format(start_date);
	String ed_dt = df.format(end_date);
	String param_name = args[3];
	String aggregator = args[4];
	String output = args[5];
	System.out.println(start_date+"\t"+end_date+"\t"+param_name+"\t"+aggregator);

	/* essential to run any spark code */
	SparkConf conf = new SparkConf().setAppName("ParameterCount").setMaster("local");
	JavaSparkContext sc = new JavaSparkContext(conf);

	/* load input data to RDD */
	JavaRDD<String> dataRDD = sc.textFile(args[0]);

	JavaPairRDD<String, Double> counts =
	    dataRDD.flatMapToPair(new PairFlatMapFunction<String, String, Double>(){
		    public Iterator<Tuple2<String, Double>> call(String value){
			
			    String lines = value.toString();
		    	String[] tuple = lines.split("\\n");
		    	String val_hrf = "value_hrf";
				String default_parameter = "parameter";
				List<Tuple2<String, Double>> retWords = new ArrayList<Tuple2<String, Double>>();
				try {
				for (int i=0;i<tuple.length;i++) {
					Object obj2 = JSONValue.parse(tuple[i]);
					JSONObject jo = (JSONObject) obj2;
					int temp = 0;
					String all_date = "Start Date: "+st_dt+" End Date: "+ed_dt+" Parameter: "+param_name+" Aggregate function: "+aggregator+" Value: ";
					String key_val="";
					Map<String, String> features = (Map<String, String>)jo.get("features");
					
					// figure out date filtering part
					String t_val = String.valueOf(jo.get("timestamp"));
					Date time_val = new java.util.Date(Long.parseLong(t_val));
					//int c1 = time_val.compareTo(start_date); // greater than 0
					//int c2 = end_date.compareTo(time_val); // greater than 0
					//int mult = c1*c2;
					
					//System.out.println(time_val.before(end_date) + "\t"+ time_val.after(start_date));
					//System.out.println(c1);
					for (Map.Entry<String, String> entry : features.entrySet()) {
						String k = entry.getKey();
						//System.out.println("qwertyuiop");
						String v = String.valueOf(entry.getValue());
						if (k.equals(default_parameter) && (v.equals(param_name)) && (time_val.after(start_date)) && time_val.before(end_date)) {
							//retWords.add(new Tuple2<String, Integer>(v, 1));
							key_val = v;
							temp=1;
							
						}
						if (k.equals(val_hrf) && temp==1) {
							Double val = Double.parseDouble(v);
							retWords.add(new Tuple2<String, Double>(all_date, val));
						}
					}
				}
				
				//for (String word:words){
				//    retWords.add(new Tuple2<String, Integer>(word, 1));
				//}
				return retWords.iterator();
				
				} catch(Exception e) {
					e.printStackTrace();
				}
				return null;
				
		    }
		}).reduceByKey(new Function2<Double, Double, Double>(){
			public Double call(Double x, Double y){
			    double total = 0;
			    
			    //System.out.println(aggregator);
			    int count = 0;
				if (aggregator.contentEquals("min")) {
					total = Math.min(x, y);
					//System.out.println("this run");		
				}
				else if (aggregator.equals("max")) {
					total = Math.max(x, y);
				}
				// average logic since x stores the collective result after each call
				// send an average of x and y. retrieve individual elements of "parameter" by multiplying by counter
				// this works as x is created by the value of total from the previous call.
				else {
					if (counter == 0) {
						total = x+y;
					} else {
						total = counter*x + y;
					}
					counter+=1;
					total = total/counter;
				}
				return total;
			}
		    });
	
	counts.saveAsTextFile(output);
	
    }
}
