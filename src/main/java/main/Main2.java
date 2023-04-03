package main;

import org.apache.spark.sql.*;
import java.io.File;
import static org.codehaus.commons.compiler.samples.DemoBase.explode;


public class Main2 {

    private static final String RULE_JSON_FILE_PATH = "./src/main/resources/test_source/rule.json";
    private static final String TMP_TOS_VSA_ND_NDS_R1_2_TSV_FILE_PATH = "./src/main/resources/test_source/tmp_tos_vsa_nd_nds_r1_2.tsv";
    private static final String TMP_TOS_VSA_ND_NDS_R3_2_TSV_FILE_PATH = "./src/main/resources/test_source/tmp_tos_vsa_nd_nds_r3_2.tsv";


    public static void main(String[] args) {
        String warehouseLocation = new File("spark-warehouse").getAbsolutePath();

        var spark = SparkSession
                .builder()
                .appName("HelloSpark")
                .config("spark.master", "local")
                .config("spark.sql.warehouse.dir", warehouseLocation)
                .enableHiveSupport()
                .getOrCreate();


        var create_vsa_nd_nds_r1_table_sql = String.join("\n"
                , "CREATE TABLE tmp_tos.vsa_nd_nds_r1 ("
                , "code_period STRING,"
                , "code_present_place STRING,"
                , "date_receipt DATE,"
                , "fid DECIMAL(38,0),"
                , "s40 DECIMAL(38,0),"
                , "year DECIMAL(38,0),"
                , "quarter DECIMAL(38,0)"
                , ") STORED AS ORC");

        spark.sql("CREATE DATABASE IF NOT EXISTS tmp_tos");
        spark.sql("SHOW DATABASES").show();
        spark.sql("DROP TABLE IF EXISTS tmp_tos.vsa_nd_nds_r1");
        spark.sql(create_vsa_nd_nds_r1_table_sql);
        spark.sql("SHOW TABLES FROM tmp_tos");
        spark.sql("INSERT INTO TABLE tmp_tos.vsa_nd_nds_r1 VALUES ('first', 'second', Current_date, 1.28, 2.2, 3.3, 4.4)");
        spark.sql("SELECT * FROM tmp_tos.vsa_nd_nds_r1").show();

        // read json
        Dataset<Row> peopleDF = spark.read().option("multiline", true).format("json").load(RULE_JSON_FILE_PATH);
        peopleDF.printSchema();
        peopleDF.select("joins").show();
        peopleDF.select("joins", "joins.table_left").show();
    }
}
