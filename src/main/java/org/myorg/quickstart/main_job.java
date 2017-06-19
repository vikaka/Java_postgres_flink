package org.myorg.quickstart;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;


import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;


public class main_job {


    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        Properties properties = new Properties();

        properties.setProperty("bootstrap.servers", "34.203.160.240:9092");
        properties.setProperty("group.id", "test");
        final List a1 = new ArrayList();

        DataStream stream = env.addSource(new FlinkKafkaConsumer010<>("test", new SimpleStringSchema(), properties))
                .map(new MapFunction<String, Tuple2<String,Double>>() {
                    @Override
                    public Tuple2<String,Double> map(String line) throws Exception {
                        String[] words = line.split(",");
                        Double price = Double.parseDouble(words[1]);

                        return new Tuple2(words[0],price);
                    }
                });


        WindowedStream k = stream.keyBy(0)
                .timeWindow(Time.seconds((2)));

        DataStream r = k.reduce(new ReduceFunction<Tuple2<String,Double>>() {
            public Tuple2<String,Double> reduce(Tuple2<String, Double> v1, Tuple2<String, Double> v2) {
                return new Tuple2(v1.f0, (v1.f1 + v2.f1)/2);
            } });

        r.addSink(new SinkFunction<Tuple2<String,Double>>() {
            final public HashMap hm = new HashMap();

            @Override
            public void invoke(Tuple2<String,Double> o) throws Exception {
                hm.put(o.f0,o.f1);
                System.out.println(hm);
                        }});

        stream.addSink(new SinkFunction<Tuple2<String,Double>>() {
            @Override
            public void invoke(Tuple2<String,Double> vk) throws Exception {
                System.out.println(hm);
                /*System.out.println(vk.f0);
                System.out.println(vk.f1);
                if (hm.containsKey(vk.f0)) {
                    double balance = ((Double) hm.get(vk.f0)).doubleValue();
                    if (balance > vk.f1) {
                        System.out.println("greater" + vk.f0);
                    } else {
                        System.out.println("lower" + vk.f1);
                    }
                }
                else{System.out.println("No Match");}*/
            }});

        /*
        DataStreamSink windows = stream.keyBy(0)
                .timeWindow(Time.seconds(2))
                //.window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .reduce(new ReduceFunction<Tuple2<String,Double>>() {
                    public Tuple2<String,Double> reduce(Tuple2<String, Double> v1, Tuple2<String, Double> v2) {
                return new Tuple2(v1.f0, (v1.f1 + v2.f1)/2);
            } }).addSink(new SinkFunction() {
                    @Override
                    public void invoke(Object o) throws Exception {
                        String average_1 = o.toString();
                        String[] avg_split = average_1.split(",");
                        //System.out.println(avg_split[0]+avg_split[1]);
                        //hm.put(avg_split[0],avg_split[1]);
                        average_1 = avg_split[1];
                        }
                });

        DataStreamSink actual = stream.addSink(new SinkFunction() {
            @Override
            public void invoke(Object k) throws Exception {
                String words = k.toString();
                String[] stream_1 = words.split(",");
                System.out.println(average_1+stream_1[1]);
            }
        });
                /*if (hm.containsKey(stream_1[0])) {
                    String price = hm.get(stream_1[0]).toString();
                    double pric = Double.parseDouble(price);
                    if (pric > Double.parseDouble(stream_1[1])) {
                        System.out.println(stream_1[0] + "greater");
                    } else {
                        System.out.println(stream_1[0] + "lesser");
                    }
                }
                else {System.out.println("Inititalizing");}
            }
        });




        /* db file load and query
        StreamTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env) ;

        CsvTableSource csvTableSource = new CsvTableSource(
                "D:/user_settings.csv",
                new String[] { "uid", "name", "buy", "sell","funds" },
                new TypeInformation<?>[] {
                        BasicTypeInfo.INT_TYPE_INFO,
                        BasicTypeInfo.STRING_TYPE_INFO,
                        BasicTypeInfo.DOUBLE_TYPE_INFO,
                        BasicTypeInfo.DOUBLE_TYPE_INFO,
                        BasicTypeInfo.DOUBLE_TYPE_INFO
                },
                ",",    // fieldDelim
                "\n",    // rowDelim
                null,   // quoteCharacter
                true,   // ignoreFirstLine
                "%",    // ignoreComments
                false); // lenient
        TypeInformation[] fieldTypes = new TypeInformation[] {
                BasicTypeInfo.INT_TYPE_INFO,
                BasicTypeInfo.STRING_TYPE_INFO,
                BasicTypeInfo.DOUBLE_TYPE_INFO,
                BasicTypeInfo.DOUBLE_TYPE_INFO,
                BasicTypeInfo.DOUBLE_TYPE_INFO
        };
        RowTypeInfo rowTypeInfo = new RowTypeInfo(fieldTypes);

        tableEnv.registerTableSource("Customers",csvTableSource);
        Table in2 = tableEnv.sql("select * from Customers where name = 'eefaf'");
        DataStreamSink<Row> dsRow = tableEnv.toAppendStream(in2, Row.class).print();



        */

//Execute program
        env.execute("streaming job");




    }

}


