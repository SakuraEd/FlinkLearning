package com.Xueyuhao.wc;
/**
 * Created by XueYuhao on 2022/2/13 15:18
 */

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @ClassName:StreamWordCount
 * @Description:
 * @Author: XueYuhao [1315067518@qq.com]
 * @Version: 1.0
 */
public class StreamWordCount {
    public static void main(String[] args) throws Exception {
        //创建流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置并行度
        env.setParallelism(6);

        //从文件中读取数据 singleOutPut算子
        String inputPath = "E:\\FlinkDemo\\src\\main\\resources\\wordcountText";
        DataStreamSource<String> inputDataSource =  env.readTextFile(inputPath);

        //基于数据流转换数据,流数据使用keyby算子
        SingleOutputStreamOperator<Tuple2<String, Integer>> resultStream = inputDataSource.flatMap(new WordCount.flatMapperFunction())
                .keyBy(0)
                .sum(1);

        resultStream.print();

        //执行任务
        env.execute();
        //并行子任务
    }
}
