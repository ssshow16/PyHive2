import com.nexr.pyhive.hadoop.FileSystemUtils;
import com.nexr.pyhive.hive.HiveJdbcClient;
import com.nexr.pyhive.model.DataFrameModel;

import java.util.Properties;

/**
 * Created by bruceshin on 11/6/14.
 */
public class Main {

    public static void main(String[] arg){

        try {
            HiveJdbcClient client = new HiveJdbcClient(true);
            client.connect("127.0.0.1",10000,"default", new Properties());
            DataFrameModel dfm = client.query("select * from iris limit 4");
            dfm.getColumnNames();
        }catch(Exception e){
            e.printStackTrace();
        }
    }
}
