package ideal;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args )
    {

        Runtime runtime = Runtime.getRuntime();
        String com = "java -jar D:\\GITRepo\\event\\target\\event.jar 1 1";
        int res = 0;
        try {
            Process pr = runtime.exec(com);
            BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(pr.getInputStream()));
            String line =null;
            while((line = bufferedReader.readLine())!=null){
                System.out.println(line);
            }
            res = pr.waitFor();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        System.out.println(res);
    }
}
