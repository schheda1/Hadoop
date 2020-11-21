import org.json.JSONObject;
import java.io.File;
import java.io.FileNotFoundException;
import java.util.Scanner;

public class Test {
    public static void main(String args[]){
       try{
           File obj = new File("aot-small-10k.json");
           Scanner sc = new Scanner(obj);
           int i=1;
           while(i<3){
                String data = sc.nextLine();
                JSONObject obj1 = new JSONObject(data);
                String features = obj1.getString("features");
                System.out.println(features.toString());
                i++;
           }
           JSONObject jo = new JSONObject("{ \"abc\" : \"def\" }");
           System.out.println(jo.toString());
       } catch (FileNotFoundException fe) {
           fe.printStackTrace();
       }
    }
}
