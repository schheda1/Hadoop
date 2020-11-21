import java.io.FileReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Iterator; 
import java.util.Map; 
import java.io.BufferedReader;
import org.json.simple.JSONArray; 
import org.json.simple.JSONObject; 
import org.json.simple.parser.*;
import org.json.simple.JSONValue;
import org.json.simple.*;

public class Test {
    public static void main(String args[]) throws FileNotFoundException, IOException {
       BufferedReader br; 
        try{
           br = new BufferedReader(new FileReader("aot-small-10k.json"));
            String line = br.readLine();
            int i=0;
            while(i<2){
                //JSONObject obj = new JSONObject(line);
                Object obj2=JSONValue.parse(line);
                //Object obj = new JSONParser().parse(line);
                JSONObject jo = (JSONObject) obj2;
                Map features = ((Map)jo.get("features"));
                // iterating address Map 
                Iterator<Map.Entry> itr1 = features.entrySet().iterator(); 
                while (itr1.hasNext()) { 
                    Map.Entry pair = itr1.next(); 
                    System.out.println(pair.getKey() + " : " + pair.getValue()); 
                } 
                //double latitude = (double) jo.get("latitude");
                double latitude = (double) jo.get("latitude");
                System.out.print(latitude);
                i++;
                line = br.readLine();
            }
            
           //Object obj = new JSONParser().parse(new FileReader("aot-small-10k.json"));
           
       } catch (Exception e) {
           //fe.printStackTrace();
            throw e;
       }
    }
}
