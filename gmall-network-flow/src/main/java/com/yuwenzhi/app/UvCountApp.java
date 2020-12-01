package com.yuwenzhi.app;

import com.yuwenzhi.bean.UserBehavior;
import com.yuwenzhi.bean.UvCount;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.util.HashSet;
import java.util.Iterator;

/**
 * @version 1.0
 * @description:
 * @author: 宇文智
 * @date 2020/11/30 10:31
 */
public class UvCountApp {
    //需求：网站独立访客数（UV）的统计（使用hashset去重）: 一个小时内的网站访客量
    public static void main(String[] args) throws Exception {
        //flink 流处理
        //1. 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //1.2 设置并行度
        env.setParallelism(1);
        //1.3 设置时间语义为事件时间
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        //2. 从文件中读取数据转换为javaBean,同时提取事件时间
        SingleOutputStreamOperator<UserBehavior> inputDS = env.readTextFile("input/UserBehavior.csv")
                .map(line -> {
                    String[] split = line.split(",");
                    return new UserBehavior(Long.parseLong(split[0]),
                            Long.parseLong(split[1]),
                            Integer.parseInt(split[2]),
                            split[3],
                            Long.parseLong(split[4]));
                }).assignTimestampsAndWatermarks(
                        //数据中时间是单调递增的，所以不用设置watermark延时，用一个增量watermark即可
                        new AscendingTimestampExtractor<UserBehavior>() {
                    @Override
                    public long extractAscendingTimestamp(UserBehavior userBehavior) {
                        long eventTime = userBehavior.getTimestamp() * 1000L;
                        return eventTime;
                    }
                });
        //每条记录即是一条访问,开窗，聚合统计（简单使用hashset去重）
        SingleOutputStreamOperator<UvCount> oneHourUvCount = inputDS
                .timeWindowAll(Time.hours(1))
                //使用apply全量聚合函数： 一次聚合窗口中的数据；
                //（增量聚合函数： reduce(reduceFunc()),aggregate(aggFunc()),sum(),min(),max()等）
                //来一条数据、计算一次
                .apply(new CountUvWindowFunc());

        oneHourUvCount.print("one hour");


        //启动任务
        env.execute();

    }

    //out: UvCount
    //实现全量窗口聚合函数
    private static class CountUvWindowFunc implements AllWindowFunction<UserBehavior, UvCount, TimeWindow> {

        @Override
        public void apply(TimeWindow timeWindow, Iterable<UserBehavior> iterable, Collector<UvCount> collector) throws Exception {
            //统计网站的总访客量 -> UvCount(uv,windowEnd,count)
            String uv = "uv";
            String winEnd = new Timestamp(timeWindow.getEnd()).toString();

            //使用 HashSet 达到去重的效果
            HashSet<Long> userIds = new HashSet<>();

            for (UserBehavior userBehavior : iterable) {
                userIds.add(userBehavior.getUserId());
            }

            long size = userIds.size();

            collector.collect(new UvCount(uv,winEnd,size));

        }
    }
}
