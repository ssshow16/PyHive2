import com.nexr.pyhive.hadoop.FileSystemUtils;

/**
 * Created by bruceshin on 11/6/14.
 */
public class Main {

    public static void main(String[] arg){

        try {
            FileSystemUtils.du("/rhive/data/bruceshin/iris_4de5a5b9c1832fb5ecaa16b79028a3","hdfs://localhost:9000","bruceshin");
        }catch(Exception e){
            e.printStackTrace();
        }
    }
}
