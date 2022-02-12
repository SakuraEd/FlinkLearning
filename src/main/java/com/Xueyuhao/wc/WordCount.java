package com.Xueyuhao.wc;
/**
 * Created by XueYuhao on 2022/2/12 22:53
 */

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * @ClassName:WordCount
 * @Description:
 * @Author: XueYuhao [1315067518@qq.com]
 * @Version: 1.0
 */

//批处理word count
public class WordCount {
    public static void main(String[] args) throws Exception{
        //创建执行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        //从文件中读取数据
        String inputPath = "E:\\FlinkDemo\\src\\main\\resources\\wordcountText";
        DataSource<String> inputDataSource =  env.readTextFile(inputPath);

        //对数据集进行处理
        DataSet<Tuple2<String , Integer>> countResult = inputDataSource.flatMap(new flatMapperFunction())
                .groupBy(0) //按照第一个位置word分组
                .sum(1);
        countResult.print();
    }

    //自定义flat Mapper Function类
    public static class flatMapperFunction implements FlatMapFunction<String, Tuple2<String , Integer>> {
        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> collector) throws Exception {
            //按照空格分词
            String[] words = value.split(" ");
            for(String word: words){
                collector.collect(new Tuple2<>(word, 1));
            }
        }
    }
}
