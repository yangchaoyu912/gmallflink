package com.yuwenzhi.app;

import com.yuwenzhi.bean.UserBehavior;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;


/**
 * @version 1.0
 * @description:
 * @author: 宇文智
 * @date 2020/11/28 15:49
 */
public class PageViewApp {
    //需求： 每一小时求网站总浏览量
    public static void main(String[] args) throws Exception {
        //flink 流处理
        //1. 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //1.2 设置并行度
        env.setParallelism(8);   //会出现数据倾斜
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        SingleOutputStreamOperator<UserBehavior> userDS = env.readTextFile("input/UserBehavior.csv")
                .map(line -> {
                    String[] fileds = line.split(",");
                    return new UserBehavior(Long.parseLong(fileds[0]),
                            Long.parseLong(fileds[1]),
                            Integer.parseInt(fileds[2]),
                            fileds[3],
                            Long.parseLong(fileds[4]));
                })
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<UserBehavior>() {
                    @Override
                    public long extractAscendingTimestamp(UserBehavior element) {
                        return element.getTimestamp() * 1000L;
                    }
                });
        //按 “pv" 进行过滤，按照itemId分组，开一小时的滚动窗口，聚合
        SingleOutputStreamOperator<Tuple2<String, Integer>> pv = userDS.filter(data -> "pv".equals(data.getBehavior()))
                .map(new MapFunction<UserBehavior, Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> map(UserBehavior userBehavior) throws Exception {
                        return new Tuple2("pv", 1);
                    }
                }).keyBy(0).timeWindow(Time.hours(1)).sum(1);

        pv.print();

        //启动任务
        env.execute();
    }
}
