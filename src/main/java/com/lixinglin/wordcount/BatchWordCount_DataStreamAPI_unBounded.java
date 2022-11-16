package com.lixinglin.wordcount;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class BatchWordCount_DataStreamAPI_unBounded {
    public static void main(String[] args) throws Exception {
        //1.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //从参数中提取主机名和端口号
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        String host = parameterTool.get("host");
        Integer port = parameterTool.getInt("port");


        //2.读取文本流
        DataStreamSource<String> stringDataStreamSource = env.socketTextStream(host, port);
        DataStreamSource<String> stringDataStreamSource1 = env.socketTextStream(host, port);
        //3.将每行数据进行分词，转换伪二元组类型
        SingleOutputStreamOperator<Tuple2<String, Long>> wordAndOneTuple = stringDataStreamSource.flatMap((String line, Collector<Tuple2<String, Long>> out) -> {
            //一行文本分词  将每个单词转为二元组输出
            for (String word : line.split(" ")) {
                out.collect(Tuple2.of(word, 1l));
            }
        }).returns(Types.TUPLE(Types.STRING, Types.LONG));

        //4.按照word进行分组

        KeyedStream<Tuple2<String, Long>, String> wordAndOneGroup = wordAndOneTuple.keyBy(data -> data.f0);


        //5.分组内进行聚合
        SingleOutputStreamOperator<Tuple2<String, Long>> sum = wordAndOneGroup.sum(1);

        //6.打印结果
        sum.print();

        //启动执行
        env.execute();
    }
}
