package org.myorg.quickstart;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;


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


        DataStream stream = env.addSource(new FlinkKafkaConsumer010<>("test", new SimpleStringSchema(), properties))
                .map(new MapFunction<String, Tuple2<String,Double>>() {
                    @Override
                    public Tuple2<String,Double> map(String line) throws Exception {
                        String[] words = line.split(",");
                        Double price = Double.parseDouble(words[1]);

                        return new Tuple2(words[0],price);
                    }
                });

        stream.print();

        DataStream windows = stream.keyBy(0)
                .timeWindow(Time.seconds(2))
                //.window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .reduce(new ReduceFunction<Tuple2<String,Double>>() {
                    public Tuple2<String,Double> reduce(Tuple2<String, Double> v1, Tuple2<String, Double> v2) {
                        return new Tuple2(v1.f0, (v1.f1 + v2.f1)/2);
                    } });



        stream.keyBy(0).flatMap(new CountWindowAverage()).print();


/*        ConnectedStreams conn = stream.connect(windows);
                conn.map(new CoMapFunction<Tuple2<String,Double>,Tuple2<String,Double>,String>() {
                    private ValueState<Double> sum;


                    @Override
            public String map1(Tuple2<String,Double> v1) throws Exception {
                        ValueState<Double> sum;
                        while (v1.f1 == null){System.out.println("null");}
                        sum.update(v1.f1);
                return new String("wasste");
                    }
            @Override
                    public String map2(Tuple2<String,Double> v2 ) throws Exception{
                        System.out.println(sum.value()+v2.f0+v2.f1);
                        sum.clear();
                return new String("test");
                        /*if (sum.value() > v2.f1) {
                            System.out.println("greater" + sum.value() + v2.f0);
                            return new String("test");
                        } else {
                            System.out.println(sum.value() + v2.f0 + "lesser");
                            return new String("test");
                        }
                    }



        });
*/
        /*WindowedStream k = stream.keyBy(0)
                .timeWindow(Time.seconds((2)));


        DataStream r = k.reduce(new ReduceFunction<Tuple2<String,Double>>() {
            public Tuple2<String,Double> reduce(Tuple2<String, Double> v1, Tuple2<String, Double> v2) {
                return new Tuple2(v1.f0, (v1.f1 + v2.f1)/2);
            } });

        r.addSink(new SinkFunction<Tuple2<String,Double>>() {
            final public HashMap hm = new HashMap();

            @Override
            public void invoke(Tuple2<String,Double> o) throws Exception {
                //System.out.println(hm);
                        }});

        stream.addSink(new SinkFunction<Tuple2<String,Double>>() {
            @Override
            public void invoke(Tuple2<String,Double> vk) throws Exception {
                //System.out.println(sum.value());
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
                else{System.out.println("No Match");}
            }});
                */

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

    public static class CountWindowAverage extends RichFlatMapFunction<Tuple2<String, Double>, Tuple2<String, Double>> {

        /**
         * The ValueState handle. The first field is the count, the second field a running sum.
         */
        private transient ValueState<Tuple2<String, Double>> sum;
        private int counts = 0;
        @Override
        public void flatMap(Tuple2<String, Double> input, Collector<Tuple2<String, Double>> out) throws Exception {

            // access the state value
            Tuple2<String, Double> currentSum = sum.value();

            // update the count
            counts += 1;

            // add the second field of the input value
            currentSum.f1 += input.f1;

            // update the state
            sum.update(currentSum);

            // if the count reaches 2, emit the average and clear the state
            if (counts >= 2) {
                out.collect(new Tuple2<>("average", currentSum.f1 / counts));
                sum.clear();
                counts = 0;
            }
        }
        @Override
        public void open(Configuration config) {
            ValueStateDescriptor<Tuple2<String, Double>> descriptor =
                    new ValueStateDescriptor<Tuple2<String, Double>>(
                            "average", // the state name
                            TypeInformation.of(new TypeHint<Tuple2<String, Double>>() {}), // type information
                            Tuple2.of("name", 0.5)); // default value of the state, if nothing was set
            sum = getRuntimeContext().getState(descriptor);
        }
    }


}


