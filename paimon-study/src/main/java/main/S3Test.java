package main;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class S3Test {

    static String createCatalog =
            "CREATE CATALOG my_catalog WITH (\n"
//                    + "    'type' = 'paimon-external',\n"
                    + "    'type' = 'paimon',\n"
                    + "    'warehouse' = 's3://paimon/test',\n"
                    + "    's3.endpoint' = 'http://192.168.85.131:32308/',\n"
                    + "    's3.access-key' = '8mTteAmJvAPVTBb6V17g',\n"
                    + "    's3.secret-key' = 'Yot3zbAKKY4D6pvmnosSNvnMiNeZZMmaAvuipFQ1'\n"
                    + ");";

    static String createTable =
            "CREATE  TABLE IF  NOT EXISTS  word_count2 (\n"
                    + "    word STRING PRIMARY KEY NOT ENFORCED,\n"
                    + "    cnt BIGINT\n"
                    + ");";

    static String showTable="show tables";

    static String createDataGenTable= "CREATE TEMPORARY TABLE word_table (\n" +
            "    word STRING\n" +
            ") WITH (\n" +
            "    'connector' = 'datagen',\n" +
            "    'fields.word.length' = '5'\n" +
            ");";

    public static void main(String[] args) {
        System.out.println(createCatalog);
        System.out.println(createDataGenTable);
        Configuration conf = new Configuration();
        conf.setString("execution.checkpointing.interval","10 s");
        conf.setString("taskmanager.memory.process.size","4096m");
        conf.setString("taskmanager.memory.process.size","4096m");
        conf.setString("taskmanager.numberOfTaskSlots","8");
        conf.setString("parallelism.default","8");
        conf.setString("taskmanager.cpu.cores","4");
        conf.setString("taskmanager.memory.framework.heap.size","1024m");
        conf.setString("s3.endpoint","http://127.0.0.1:32308");
        conf.setString("s3.access-key","8mTteAmJvAPVTBb6V17g");
        conf.setString("s3.secret-key","Yot3zbAKKY4D6pvmnosSNvnMiNeZZMmaAvuipFQ1");
        conf.setString("state.checkpoints.dir:","s3p://flink/checkpoints");
        conf.setString("state.savepoints.dir","s3a://flink/savepoints");
        conf.setString("state.backend.type","rocksdb");
        conf.setString("state.backend.incremental","true");
        conf.setString("state.backend.rocksdb.localdir","H:\\rocksdb");
        conf.setInteger(RestOptions.PORT,8085);
        LocalStreamEnvironment localEnvironment = StreamExecutionEnvironment.createLocalEnvironment(conf);
        localEnvironment.enableCheckpointing(10000);
        StreamTableEnvironment tenv = StreamTableEnvironment.create(localEnvironment);
//        TableEnvironment tenv =
//                TableEnvironment.create(
//                        EnvironmentSettings.newInstance().inStreamingMode().build());
        tenv.executeSql(createCatalog);

        tenv.executeSql("use CATALOG my_catalog");
//        tenv.executeSql("show databases").print();
        tenv.executeSql(createDataGenTable);
        tenv.executeSql(createTable);
//        tenv.executeSql(showTable).print();
//        tenv.executeSql("SELECT word, COUNT(*) FROM word_table GROUP BY word").print();
        tenv.executeSql("INSERT INTO word_count2 SELECT word, COUNT(*) FROM word_table GROUP BY word;");
//        System.out.println(tableResult.getJobClient().get().getJobStatus());
//        tenv.executeSql("select * from word_count").print();

    }
}
