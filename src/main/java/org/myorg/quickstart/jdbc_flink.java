package org.myorg.quickstart;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.jdbc.JDBCInputFormat;
import org.apache.flink.api.java.typeutils.RowTypeInfo;

import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.BatchTableEnvironment;


/**
 * Created by Vishesh Kakarala on 6/15/2017.
 */
public class jdbc_flink {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        //BatchTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);
        BatchTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env) ;

        TypeInformation[] fieldTypes = new TypeInformation[] {
                BasicTypeInfo.INT_TYPE_INFO,
                BasicTypeInfo.STRING_TYPE_INFO,
                //BasicTypeInfo.STRING_TYPE_INFO,
                //BasicTypeInfo.DOUBLE_TYPE_INFO,
                //BasicTypeInfo.INT_TYPE_INFO
        };
        RowTypeInfo rowTypeInfo = new RowTypeInfo(fieldTypes);

        DataSet dbData =
                env.createInput(
                        // create and configure input format
                        JDBCInputFormat.buildJDBCInputFormat()
                                .setDrivername("org.postgresql.Driver")
                                .setDBUrl("jdbc:postgresql://34.225.139.150:5432/testdb?user=postgres&password=kakarala&ssl=true&sslfactory=org.postgresql.ssl.NonValidatingFactory")
                                .setUsername("postgres")
                                .setPassword("kakarala")//.setQuery("select * from test_name where name = 'cbeeb'")
                                .setQuery("select id,name from public.test_name")
                                .setRowTypeInfo(rowTypeInfo)
                                .finish()
                        // specify type information for DataSet

                );
        tableEnv.registerDataSet("Customers",dbData,"id,name");
        Table in = tableEnv.sql("select * from Customers where name = 'cbeeb'");
        DataSet result = tableEnv.toDataSet(in,rowTypeInfo);
        result.print();
        env.execute("Word Count Example");
    }
}
