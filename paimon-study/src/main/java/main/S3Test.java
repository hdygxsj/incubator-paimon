package main;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

public class S3Test {

    static String createCatalog = "CREATE CATALOG my_catalog WITH (\n" +
            "    'type' = 'paimon',\n" +
            "    'warehouse' = 's3://paimon/test',\n" +
            "    's3.endpoint' = 'http://192.168.85.131:32308/',\n" +
            "    's3.access-key' = 'odNqeqZ5EGPByevqB9rX',\n" +
            "    's3.secret-key' = '833IRC77g9BzddWJjIystKj4Wnxvr65hugULuiK4'\n" +
            ");";
    public static void main(String[] args) {
        TableEnvironment tenv = TableEnvironment.create(EnvironmentSettings.newInstance().inStreamingMode().build());
        tenv.executeSql(createCatalog);
        tenv.executeSql("show databases").print();

    }
}
