package org.myorg.quickstart;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.io.jdbc.JDBCInputFormat;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import java.sql.*;
import java.util.Properties;



public class main_job {


    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

        //register datastreams
        Properties properties = new Properties();

        properties.setProperty("bootstrap.servers", "34.203.160.240:9092");
        properties.setProperty("group.id", "test");


        DataStream stream = env.addSource(new FlinkKafkaConsumer010<>("test", new SimpleStringSchema(), properties))
                .map(new MapFunction<String, Tuple2<String, Double>>() {
                    @Override
                    public Tuple2<String, Double> map(String line) throws Exception {
                        String[] words = line.split(",");
                        Double price = Double.parseDouble(words[1]);

                        return new Tuple2(words[0], price);
                    }
                });

        DataStream val = stream.keyBy(0).flatMap(new CountWindowAverage());
        val.flatMap(new QuerySend());

       /* final StreamTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);

        TypeInformation[] fieldTypes = new TypeInformation[]{
                BasicTypeInfo.LONG_TYPE_INFO,
                BasicTypeInfo.STRING_TYPE_INFO,
                BasicTypeInfo.DOUBLE_TYPE_INFO,
                BasicTypeInfo.DOUBLE_TYPE_INFO,
                BasicTypeInfo.DOUBLE_TYPE_INFO
        };
        RowTypeInfo rowTypeInfo = new RowTypeInfo(fieldTypes);

        DataStreamSource dbData =
                env.createInput(
                        // create and configure input format
                        JDBCInputFormat.buildJDBCInputFormat()
                                .setDrivername("org.postgresql.Driver")
                                .setDBUrl("jdbc:postgresql://34.225.139.150:5432/testdb?user=postgres&password=kakarala&ssl=true&sslfactory=org.postgresql.ssl.NonValidatingFactory")
                                .setUsername("postgres")
                                .setPassword("kakarala")//.setQuery("select * from test_name where name = 'cbeeb'")
                                .setQuery("select * from public.user_settings")
                                .setRowTypeInfo(rowTypeInfo)
                                .finish()
                        // specify type information for DataSet

                );

        tableEnv.registerDataStream("Customers", dbData, "id,name,buy,sell,funds");

        final Table vek = tableEnv.scan("Customers").select("*");
        final TupleTypeInfo<Tuple5<Long, String, Double, Double, Double>> tupleType = new TupleTypeInfo<>(
                BasicTypeInfo.LONG_TYPE_INFO,
                BasicTypeInfo.STRING_TYPE_INFO,
                BasicTypeInfo.DOUBLE_TYPE_INFO,
                BasicTypeInfo.DOUBLE_TYPE_INFO,
                BasicTypeInfo.DOUBLE_TYPE_INFO);
        DataStream vl = tableEnv.toAppendStream(vek, tupleType);
        vl.broadcast();

        val.flatMap(new FlatMapFunction<Tuple2<String, Double>, Tuple2<String, Double>>() {
            String url = "jdbc:postgresql://34.225.139.150:5432/testdb?user=postgres&password=kakarala&ssl=true&sslfactory=org.postgresql.ssl.NonValidatingFactory";
            Connection conn = DriverManager.getConnection(url);

            @Override
            public void flatMap(final Tuple2<String, Double> o, Collector<Tuple2<String, Double>> collector) throws Exception {
                if (o.f1 > 0) {
                    PreparedStatement st = conn.prepareStatement("SELECT * FROM user_settings WHERE buy <= ?");
                    st.setDouble(1, o.f1);
                    ResultSet rs = st.executeQuery();
                    while (rs.next()) {
                        System.out.print("Column 1 returned ");
                        System.out.println(rs.getString(1));
                    }
                    rs.close();
                    st.close();
                } else {
                    String url = "jdbc:postgresql://34.225.139.150:5432/testdb?user=postgres&password=kakarala&ssl=true&sslfactory=org.postgresql.ssl.NonValidatingFactory";
                    Connection conn = DriverManager.getConnection(url);
                    PreparedStatement st = conn.prepareStatement("SELECT * FROM user_settings WHERE sell >= ?");
                    st.setDouble(1, o.f1);
                    ResultSet rs = st.executeQuery();
                    while (rs.next()) {
                        System.out.print("Column 1 returned ");
                        System.out.println(rs.getString(1));
                    }
                    rs.close();
                    st.close();
                }

            }
        });



        ConnectedStreams connected1 = val.connect(vl);

        connected1.flatMap(new CoFlatMapFunction<Tuple2<String,Double>,Tuple5<Long,String,Double,Double,Double>,Tuple5<Long,String,Double,Double,Double>>(){

            private double lengthToSkip = 0;

            @Override
            public void flatMap1(Tuple2<String,Double> value1, Collector<Tuple5<Long,String,Double,Double,Double>> out) throws Exception {
                lengthToSkip = value1.f1;
                System.out.println(lengthToSkip);
            }

            @Override
            public void flatMap2(Tuple5<Long,String,Double,Double,Double> vals, Collector <Tuple5<Long,String,Double,Double,Double>> out) throws Exception {
                System.out.println(lengthToSkip);
                if (lengthToSkip > vals.f2){
                    out.collect(new Tuple5<>(vals.f0,"buy",vals.f2,vals.f3,vals.f4));
                }
                else {out.collect(new Tuple5<>(vals.f0,"sell",vals.f2,vals.f3,vals.f4));}

            }
        }).print();
*/

//Execute program
        env.execute("streaming job");


    }

    public static class CountWindowAverage extends RichFlatMapFunction<Tuple2<String, Double>, Tuple2<String, Double>> {

        /**
         * The ValueState handle. The first field is the count, the second field a running sum.
         */
        private transient ValueState<Tuple2<String, Double>> sum;
        private transient ValueState<Tuple2<String, Double>> avg1;
        private int counts = 0;

        @Override
        public void flatMap(Tuple2<String, Double> input, Collector<Tuple2<String, Double>> out) throws Exception {

            // access the state value
            Tuple2<String, Double> currentSum = sum.value();
            Tuple2<String, Double> currentavg = avg1.value();
            // update the count
            counts += 1;

            // add the second field of the input value
            currentSum.f1 += input.f1;

            // update the state
            sum.update(currentSum);


            // if the count reaches 2, emit the average and clear the state
            if (counts >= 5) {
                currentavg.f1 = currentSum.f1 / counts;
                avg1.update(currentavg);
                sum.clear();
                counts = 0;
            }
            double diff = (input.f1 - avg1.value().f1) / avg1.value().f1;
            if (diff > 0) {
            } else {
            }
            out.collect(new Tuple2<>(input.f0, diff));

        }

        @Override
        public void open(Configuration config) {
            ValueStateDescriptor<Tuple2<String, Double>> descriptor =
                    new ValueStateDescriptor<Tuple2<String, Double>>(
                            "average", // the state name
                            TypeInformation.of(new TypeHint<Tuple2<String, Double>>() {
                            }), // type information
                            Tuple2.of("name", 190.5)); // default value of the state, if nothing was set
            sum = getRuntimeContext().getState(descriptor);
            avg1 = getRuntimeContext().getState(descriptor);
        }

    }

    public static class QuerySend extends RichFlatMapFunction<Tuple2<String, Double>, Tuple2<String, Double>> {

        private Connection conn;

        @Override
        public void flatMap(Tuple2<String, Double> input, Collector<Tuple2<String, Double>> out) throws Exception {

            PreparedStatement st = conn.prepareStatement("SELECT * FROM user_settings WHERE sell > ?");
            st.setDouble(1, input.f1*-1);
            ResultSet rs = st.executeQuery();
            while (rs.next()) {
                System.out.print("Column 1 returned ");
                System.out.println(rs.getRow());
            }
            rs.close();


        }

        @Override
        public void open(Configuration config) throws SQLException {
            String url = "jdbc:postgresql://34.225.139.150:5432/testdb?user=postgres&password=kakarala&ssl=true&sslfactory=org.postgresql.ssl.NonValidatingFactory";
            conn = DriverManager.getConnection(url);
            TypeInformation[] fieldTypes = new TypeInformation[]{
                    BasicTypeInfo.LONG_TYPE_INFO,
                    BasicTypeInfo.STRING_TYPE_INFO,
                    BasicTypeInfo.DOUBLE_TYPE_INFO,
                    BasicTypeInfo.DOUBLE_TYPE_INFO,
                    BasicTypeInfo.DOUBLE_TYPE_INFO
            };
            RowTypeInfo rowTypeInfo = new RowTypeInfo(fieldTypes);




        }

        }

}









