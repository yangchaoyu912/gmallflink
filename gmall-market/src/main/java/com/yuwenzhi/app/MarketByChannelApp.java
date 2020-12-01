package com.yuwenzhi.app;

import com.yuwenzhi.bean.ChannelBehaviorCount;
import com.yuwenzhi.bean.MarketUserBehavior;
import com.yuwenzhi.source.MarketBehaviorSource;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;

/**
 * @version 1.0
 * @description:
 * @author: 宇文智
 * @date 2020/11/30 12:51
 */
//市场营销商业指标统计分析
//  -- App 市场推广分析（分渠道分析）
// : 每隔5秒钟统计最近一个小时按照渠道的推广量。
public class MarketByChannelApp {
    public static void main(String[] args) throws Exception {
        //flink 流处理
        //1. 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //1.2 设置并行度
        env.setParallelism(1);

        //2. 从自定义数据源中读取数据
        DataStreamSource<MarketUserBehavior> marketDS = env.addSource(new MarketBehaviorSource());

        //3. 过滤卸载数据，按照渠道和行为分组
        SingleOutputStreamOperator<ChannelBehaviorCount> result = marketDS.filter(data -> !"UNINSTALL".equals(data.getBehavior()))
                .keyBy("channel", "behavior")
                .timeWindow(Time.hours(1), Time.seconds(5))
                .aggregate(new MarketAggFunc(), new MarketWindowFunc());

        result.print();

        //启动任务
        env.execute();
    }

    //自定义聚合函数
    private static class MarketAggFunc implements AggregateFunction<MarketUserBehavior,Long,Long> {
        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        //来一条数据 ，自增1
        public Long add(MarketUserBehavior marketUserBehavior, Long acc) {
            return acc + 1L;
        }

        @Override
        public Long getResult(Long acc) {
            return acc;
        }

        @Override
        //会话窗口中会调用
        public Long merge(Long aLong, Long acc1) {
            return null;
        }
    }

    //将聚合结果输出（可以进行结构转换）
    public static class MarketWindowFunc implements WindowFunction<Long, ChannelBehaviorCount,Tuple, TimeWindow>{

        @Override
        public void apply(Tuple tuple, TimeWindow timeWindow, Iterable<Long> iterable, Collector<ChannelBehaviorCount> collector) throws Exception {
            String channel = tuple.getField(0);
            String behavior = tuple.getField(1);
            String windowEnd = new Timestamp(timeWindow.getEnd()).toString();
            Long count = iterable.iterator().next();

            collector.collect(new ChannelBehaviorCount(channel, behavior, windowEnd, count));
        }
    }

}
