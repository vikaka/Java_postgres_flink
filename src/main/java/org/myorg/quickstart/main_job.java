package org.myorg.quickstart;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;

import org.apache.flink.util.Collector;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.*;
import java.sql.Time;
import java.util.*;


public class main_job {


    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

        //register dataStreams
        Properties properties = new Properties();

        properties.setProperty("bootstrap.servers", "34.227.239.169:9092");
        properties.setProperty("group.id", "test");


        DataStream stream = env.addSource(new FlinkKafkaConsumer010<>("test", new SimpleStringSchema(), properties))
                .map(new MapFunction<String, Tuple3<String, Double,Timestamp>>() {
                    @Override
                    public Tuple3<String, Double,Timestamp> map(String line) throws Exception {
                        String[] words = line.split(",");
                        Double price = Double.parseDouble(words[1]);
                        Timestamp ts = Timestamp.valueOf(words[2]);
                        return new Tuple3(words[0], price,ts);
                    }
                });

        DataStream val = stream.keyBy(0).flatMap(new CountWindowAverage());
        val.print();
        //val.flatMap(new QuerySend());



//Execute program
        env.execute("streaming job");


    }

    public static class CountWindowAverage extends RichFlatMapFunction<Tuple3<String, Double,Timestamp>, Tuple4<String, Double, Double,Timestamp>> {

        /**
         * The ValueState handle. The first field is the sum, the second field a running average.
         */
        private transient ValueState<Tuple2<String, Double>> val1;
        private transient ValueState<Tuple2<String, Double>> val2;
        private transient ValueState<Tuple2<String, Double>> val3;
        private transient ValueState<Tuple2<String, Double>> val4;
        private transient ValueState<Tuple2<String, Double>> val5;
        private int flag = 1;
        private double avg;
        private double sum;
        private double diff;

        @Override
        public void flatMap(Tuple3<String, Double,Timestamp> input, Collector<Tuple4<String, Double,Double,Timestamp>> out) throws Exception {

            sum = val1.value().f1+val2.value().f1+val3.value().f1+val4.value().f1+val5.value().f1;
            avg = sum/5;
            diff = (input.f1 - avg) / avg;


            switch(flag){
                case 1:
                    val1.update(new Tuple2(input.f0,input.f1));
                    flag =2;
                    break;
                case 2:
                    val2.update(new Tuple2(input.f0,input.f1));
                    flag =3;
                    break;
                case 3:
                    val3.update(new Tuple2(input.f0,input.f1));
                    flag =4;
                    break;
                case 4:
                    val4.update(new Tuple2(input.f0,input.f1));
                    flag =5;
                    break;
                case 5:
                    val5.update(new Tuple2(input.f0,input.f1));
                    flag =1;
                    break;
            }

            if(diff == input.f1){
                diff = 0.0;
            }


            out.collect(new Tuple4<>(input.f0, input.f1,diff,input.f2));//input.f2));

        }

        @Override
        public void open(Configuration config) {
            ValueStateDescriptor<Tuple2<String, Double>> descriptor =
                    new ValueStateDescriptor<Tuple2<String, Double>>(
                            "average1", // the state name
                            TypeInformation.of(new TypeHint<Tuple2<String, Double>>() {
                            }), // type information
                            Tuple2.of("name", 0.0)); // default value of the state, if nothing was set
            val1 = getRuntimeContext().getState(descriptor);
            ValueStateDescriptor<Tuple2<String, Double>> descriptor1 =
                    new ValueStateDescriptor<Tuple2<String, Double>>(
                            "average2", // the state name
                            TypeInformation.of(new TypeHint<Tuple2<String, Double>>() {
                            }), // type information
                            Tuple2.of("name", 0.0)); // default value of the state, if nothing was set
            val2 = getRuntimeContext().getState(descriptor1);
            ValueStateDescriptor<Tuple2<String, Double>> descriptor2 =
                    new ValueStateDescriptor<Tuple2<String, Double>>(
                            "average3", // the state name
                            TypeInformation.of(new TypeHint<Tuple2<String, Double>>() {
                            }), // type information
                            Tuple2.of("name", 0.0)); // default value of the state, if nothing was set

            val3 = getRuntimeContext().getState(descriptor2);
            ValueStateDescriptor<Tuple2<String, Double>> descriptor3 =
                    new ValueStateDescriptor<Tuple2<String, Double>>(
                            "average4", // the state name
                            TypeInformation.of(new TypeHint<Tuple2<String, Double>>() {
                            }), // type information
                            Tuple2.of("name", 0.0)); // default value of the state, if nothing was set

            val4 = getRuntimeContext().getState(descriptor3);
            ValueStateDescriptor<Tuple2<String, Double>> descriptor4 =
                    new ValueStateDescriptor<Tuple2<String, Double>>(
                            "average5", // the state name
                            TypeInformation.of(new TypeHint<Tuple2<String, Double>>() {
                            }), // type information
                            Tuple2.of("name", 0.0)); // default value of the state, if nothing was set

            val5 = getRuntimeContext().getState(descriptor4);
        }

    }

    public static class QuerySend extends RichFlatMapFunction<Tuple4<String, Double, Double,Timestamp>, Tuple2<String, Double>> {

        private Connection conn;
        private Connection conn_write;

        @Override
        public void flatMap(Tuple4<String, Double, Double,Timestamp> input, Collector<Tuple2<String, Double>> out) throws Exception {
            if(input.f1<0) {
                PreparedStatement st = conn.prepareStatement("SELECT * FROM BUY WHERE sell > ?");
                st.setDouble(1, input.f2 * -1);
                ResultSet rs = st.executeQuery();
                if (!rs.next() ) {
                    System.out.println("no data");
                }
                else{
                while (rs.next()) {
                    String name = rs.getString("name");
                    Long uid = rs.getLong("UID");
                    Double Price = rs.getDouble("price");
                    Double Quantity = rs.getDouble("quantity");
                    Double cost = Price*Quantity;
                    String sell = "sell";
                    PreparedStatement ins = conn_write.prepareStatement("Insert into TRANSACTIONS values(?,?,?,?,?,?,?) ");
                    ins.setLong(1,uid);
                    ins.setString(2,name);
                    ins.setDouble(3,cost);
                    ins.setString(4,sell);
                    ins.setString(5,input.f0);
                    ins.setDouble(6,Quantity);
                    ins.setTimestamp(7,input.f3);
                    ins.executeUpdate();
                    //System.out.println(name+" "+uid+" "+Funds+" sell "+input.f0);

                }
                rs.close();}
            }
            else{
                PreparedStatement st = conn.prepareStatement("SELECT * FROM user_settings WHERE buy < ? and funds > ?");
                st.setDouble(1, input.f2 );
                st.setDouble(2, input.f1 );
                ResultSet rs = st.executeQuery();
                if (!rs.next() ) {
                    System.out.println("no data");
                }
                else {

                    while (rs.next()) {
                        String name = rs.getString("name");
                        Long uid = rs.getLong("UID");
                        Double Funds = rs.getDouble("funds");
                        String buy = "buy";
                        Double quantity = Math.floor(Funds / input.f1);
                        Double cost = quantity * Funds;
                        Double sell = rs.getDouble("sell");
                        PreparedStatement ins = conn_write.prepareStatement("Insert into TRANSACTIONS values(?,?,?,?,?,?,?) ");
                        ins.setLong(1, uid);
                        ins.setString(2, name);
                        ins.setDouble(3, cost);
                        ins.setString(4, buy);
                        ins.setString(5, input.f0);
                        ins.setDouble(6, quantity);
                        ins.setTimestamp(7, input.f3);

                        ins.executeUpdate();
                        PreparedStatement buy_sql = conn_write.prepareStatement("Insert into BUY values(?,?,?,?,?,?) ");
                        buy_sql.setLong(1, uid);
                        buy_sql.setString(2, input.f0);
                        buy_sql.setDouble(3, input.f1);
                        buy_sql.setDouble(4, quantity);
                        buy_sql.setTimestamp(5, input.f3);
                        buy_sql.setDouble(6, sell);

                        buy_sql.executeUpdate();
                        //System.out.println(name+" "+uid+" "+Funds+" buy "+input.f0);
                    }
                    rs.close();
                }

            }

        }

        @Override
        public void open(Configuration config) throws SQLException {
            try{
                Class.forName("org.postgresql.Driver");
            }
            catch(ClassNotFoundException ioe){ System.out.print("No Driver");}

            String write_url = "jdbc:postgresql://transactions-trades.ca4vkhzfvza0.us-east-1.rds.amazonaws.com:5432/transactions?user=postgres&password=kakarala&ssl=true&sslfactory=org.postgresql.ssl.NonValidatingFactory";
            String read_url = "jdbc:postgresql://replica1.ca4vkhzfvza0.us-east-1.rds.amazonaws.com:5432,replica2.ca4vkhzfvza0.us-east-1.rds.amazonaws.com:5432,replica3.ca4vkhzfvza0.us-east-1.rds.amazonaws.com:5432,replica4.ca4vkhzfvza0.us-east-1.rds.amazonaws.com:5432/transactions";

            Properties props = new Properties();
            props.setProperty("user","postgres");
            props.setProperty("password","kakarala");
            props.setProperty("ssl","true");
            props.setProperty("sslfactory","org.postgresql.ssl.NonValidatingFactory");
            props.setProperty("autoReconnect", "true");
            props.setProperty("roundRobinLoadBalance", "true");
            props.setProperty("loadBalanceHosts","true");




            conn = DriverManager.getConnection(read_url,props);
            conn_write = DriverManager.getConnection(write_url,props);
                    }


        }

}









