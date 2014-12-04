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
//            u'/user/hive/warehouse/iris'
//            u'hdfs://127.0.0.1:9000'
//            'anonymous'
//            FileSystemUtils.du("/user/hive/warehouse/iris","hdfs://127.0.0.1:9000","anonymous");
            client.connect("127.0.0.1",10000,"default", new Properties());
//            DataFrameModel dfm = client.query("desc rs_anonymous_20141125140454_88338");

            DataFrameModel dfm = FileSystemUtils.ls("/","hdfs://127.0.0.1:9000","anonymous");
            System.out.println(dfm);
//            System.out.println(dfm.next());

//            dfm.close();

//            while(dfm.next()){
//                System.out.println(dfm.getStringValue(1));
//            }

//            System.out.println(dfm);
//            System.out.println(dfm.next());

//            dfm.getColumnNames();
        }catch(Exception e){
            e.printStackTrace();
        }
    }
}
