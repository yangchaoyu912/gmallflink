package com.yuwenzhi.app;

import com.yuwenzhi.bean.OrderEvent;
import com.yuwenzhi.bean.ReceiptEvent;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
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
    //实现方式一： 使用process 状态编程，定时器  ，缺点： 比较麻烦
    //两条流各自等待对方5秒
public class OrderReceiptWithConnect {
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

        //3. 两个流按照订单流水id进行分组，使用connect再将相同的key做连接
        SingleOutputStreamOperator<Tuple2<OrderEvent, ReceiptEvent>> result = orderEventDS.keyBy(OrderEvent::getTxId).connect(receiptEventDS.keyBy(ReceiptEvent::getTxId))
                .process(new OrderPayRceiptCoProcessFunc());
        //4.打印数据
        result.print("payAndReceipt");
        result.getSideOutput(new OutputTag<String>("payButNoReceipt") {
        }).print("payButNoReceipt");
        result.getSideOutput(new OutputTag<String>("receiptButNoPay") {
        }).print("receiptButNoPay");

        //启动任务
        env.execute();
    }

    private static class OrderPayRceiptCoProcessFunc extends CoProcessFunction<OrderEvent,ReceiptEvent, Tuple2<OrderEvent,ReceiptEvent>> {

        //定义两个状态保存，订单信息和到账信息
        private ValueState<OrderEvent> orderEventValueState;
        private ValueState<ReceiptEvent> receiptEventValueState;
        private ValueState<Long> tsState;

        //初始化
        @Override
        public void open(Configuration parameters) throws Exception {
           orderEventValueState = getRuntimeContext().getState(new ValueStateDescriptor<OrderEvent>("order-state", OrderEvent.class));
           receiptEventValueState = getRuntimeContext().getState(new ValueStateDescriptor<ReceiptEvent>("receipt-state",ReceiptEvent.class));
            tsState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("ts-state",Long.class));
        }

        @Override
        //处理订单日志流中的元素
        public void processElement1(OrderEvent orderEvent, Context context, Collector<Tuple2<OrderEvent, ReceiptEvent>> collector) throws Exception {
            //判断对应的到账日志流中的元素是否到了
            if(receiptEventValueState.value() == null){
                //没有到，等待5秒（注册定时器），5秒后从侧输出流中打印警告信息
                orderEventValueState.update(orderEvent);

                long ts = (orderEvent.getEventTime() + 5) * 100L;
                context.timerService().registerEventTimeTimer(ts);

                tsState.update(ts);
            }else{
                //到了，将两个流进行合并
                collector.collect(new Tuple2<>(orderEvent,receiptEventValueState.value()));

                //删除定时器
                context.timerService().deleteEventTimeTimer(tsState.value());
                tsState.clear();
                receiptEventValueState.clear();
                orderEventValueState.clear();
            }
        }

        @Override
        //处理到账日志流中的元素
        public void processElement2(ReceiptEvent receiptEvent, Context context, Collector<Tuple2<OrderEvent, ReceiptEvent>> collector) throws Exception {
            //判断对应的订单日志流中的元素是否到了
            if(orderEventValueState.value() == null){
                //没有到，等待5秒（注册定时器），5秒后从侧输出流中打印警告信息
                receiptEventValueState.update(receiptEvent);

                long ts = (receiptEvent.getTimestamp() + 5) * 100L;
                context.timerService().registerEventTimeTimer(ts);

                tsState.update(ts);
            }else{
                //到了，将两个流进行合并
                collector.collect(new Tuple2<>(orderEventValueState.value(),receiptEvent));

                //删除定时器
                context.timerService().deleteEventTimeTimer(tsState.value());
                tsState.clear();
                receiptEventValueState.clear();
                orderEventValueState.clear();
            }
        }

        @Override
        //定时器触发后，肯定是两个流中其中一个流没有连接上
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<Tuple2<OrderEvent, ReceiptEvent>> out) throws Exception {
            if(orderEventValueState.value()!=null){
                //只有支付没有到账数据
                ctx.output(new OutputTag<String>("payButNoReceipt") {
                }, orderEventValueState.value().getTxId() + "只有支付没有到账！");
            }else {
                //只有到账没有支付数据
                ctx.output(new OutputTag<String>("receiptButNoPay") {
                }, receiptEventValueState.value().getTxId() + "只有到账没有支付！");
            }

            //清空状态
            orderEventValueState.clear();
            receiptEventValueState.clear();
            tsState.clear();
        }
    }
}
