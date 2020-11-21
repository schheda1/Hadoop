import java.io.FileReader; 
import java.util.Iterator; 
import java.util.Map; 
import java.io.BufferedReader;
import org.json.simple.JSONArray; 
import org.json.simple.JSONObject; 
import org.json.simple.parser.*;

public class Test {
    public static void main(String args[]){
       BufferedReader br; 
        try{
           br = = new BufferedReader(new FileReader("aot-small-10k.json"));
            String line = br.readLine();
            int i=0;
            while(i<2){
                //JSONObject obj = new JSONObject(line);
                Object obj = new JSONParser().parse(line);
                JSONObject jo = (JSONObject) obj;
                Map features = ((Map)jo.get("features"));
                // iterating address Map 
                Iterator<Map.Entry> itr1 = address.entrySet().iterator(); 
                while (itr1.hasNext()) { 
                    Map.Entry pair = itr1.next(); 
                    System.out.println(pair.getKey() + " : " + pair.getValue()); 
                } 
                double latitiude = (double) jo.get("latitude");
                System.out.print(latitude);
                i++;
                line = br.readLine();
            }
            
           //Object obj = new JSONParser().parse(new FileReader("aot-small-10k.json"));
           
       } catch (FileNotFoundException fe) {
           fe.printStackTrace();
       }
    }
}
