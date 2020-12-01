package com.yuwenzhi.app;

import com.yuwenzhi.bean.OrderEvent;
import com.yuwenzhi.bean.ReceiptEvent;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * @version 1.0
 * @description:
 * @author: 宇文智
 * @date 2020/12/1 11:24
 */
//订单支付实时监控模块
//指标： 实时对账 （来自两条流的订单交易匹配）
    //实现方式二： 使用Join
    //两条流各自等待对方5秒
public class OrderReceiptWithJoin {
    public static void main(String[] args) throws Exception {
        //flink 流处理
        //1. 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //1.2 设置并行度
        env.setParallelism(1);
        //1.3 设置时间语义为事件时间
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        //2.读取文件数据创建流,转换为JavaBean,提取事件时间
        //2.1 订单日志流
        SingleOutputStreamOperator<OrderEvent> orderEventDS = env.readTextFile("input/OrderLog.csv")
                .map(line -> {
                    String[] fields = line.split(",");
                    return new OrderEvent(Long.parseLong(fields[0]),
                            fields[1],
                            fields[2],
                            Long.parseLong(fields[3]));
                })
                //过滤出有订单流水的
                .filter(data -> !"".equals(data.getTxId()))
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<OrderEvent>() {
                    @Override
                    public long extractAscendingTimestamp(OrderEvent element) {
                        return element.getEventTime() * 1000L;
                    }
                });

        //2.2 订单到账流
        SingleOutputStreamOperator<ReceiptEvent> receiptEventDS = env.readTextFile("input/ReceiptLog.csv")
                .map(line -> {
                    String[] fields = line.split(",");
                    return new ReceiptEvent(fields[0], fields[1], Long.parseLong(fields[2]));
                })
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<ReceiptEvent>() {
                    @Override
                    public long extractAscendingTimestamp(ReceiptEvent element) {
                        return element.getTimestamp() * 1000L;
                    }
                });
        //3.使用join, 订单流向前扫描（到账流）5秒，向后扫描（到账流）5秒数据
        SingleOutputStreamOperator<Tuple2<OrderEvent, ReceiptEvent>> result = orderEventDS.keyBy(OrderEvent::getTxId).intervalJoin(receiptEventDS.keyBy(ReceiptEvent::getTxId))
                .between(Time.seconds(-5), Time.seconds(5))
                .process(new OrderReceiptProcessJoinFunc());

        //4.打印数据
        result.print();


        //启动任务
        env.execute();
    }

    //join的局限，只能输出join上的，超时订单不能处理。没有状态编程和使用定时器，灵活
    private static class OrderReceiptProcessJoinFunc extends ProcessJoinFunction<OrderEvent, ReceiptEvent, Tuple2<OrderEvent, ReceiptEvent>> {
        @Override
        public void processElement(OrderEvent orderEvent, ReceiptEvent receiptEvent, Context context, Collector<Tuple2<OrderEvent, ReceiptEvent>> collector) throws Exception {
            collector.collect(new Tuple2<>(orderEvent,receiptEvent));
        }
    }
}
