package com.yuwenzhi.app;

import com.yuwenzhi.bean.OrderEvent;
import com.yuwenzhi.bean.OrderResult;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * @version 1.0
 * @description:
 * @author: 宇文智
 * @date 2020/11/30 21:11
 */
//订单支付实时监控
//使用flink 完成本该javaee完成的订单超时15分钟后取消的业务
    //使用状态编程的方式
public class OrderTimeOutAppWithState {

    public static void main(String[] args) throws Exception {
        //flink 流处理
        //1. 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //1.2 设置并行度
        env.setParallelism(1);
        //1.3 设置时间语义为事件时间
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        //2.读取文件数据创建流,转换为JavaBean,提取事件时间
        SingleOutputStreamOperator<OrderEvent> orderEventDS = env.readTextFile("input/OrderLog.csv")
                .map(line -> {
                    String[] fields = line.split(",");
                    return new OrderEvent(Long.parseLong(fields[0]),
                            fields[1],
                            fields[2],
                            Long.parseLong(fields[3]));
                }).assignTimestampsAndWatermarks(new AscendingTimestampExtractor<OrderEvent>() {
                    @Override
                    public long extractAscendingTimestamp(OrderEvent element) {
                        return element.getEventTime() * 1000L;
                    }
                });
        //3. 使用状态编程的实现方式
        // 原数据包含两种数据，1. create -> pay （两个动作在15分钟内完成） => 正常支付数据
        //                   2. create ,15分钟后 没有 pay  => 超时支付数据
        //根据订单id分组，然后使用状态和定时器完成 （process API）
        //状态： 1. 保存是否之前 创建了订单 的 flag 2. 定时器的时间
        //定时器： 15分钟后触发，正常支付时，删除定时器，如果触发就是超时支付数据
        SingleOutputStreamOperator<OrderResult> result = orderEventDS.keyBy(OrderEvent::getOrderId).process(new OrderTimeOutProcess());

        //6. 打印数据
        result.print("normal payed");
        // 超时订单
        DataStream<OrderResult> sideOutput = result.getSideOutput(new OutputTag<OrderResult>("payTimeOut"){});
        sideOutput.print("timeOut");
        env.execute();

    }

    private static class OrderTimeOutProcess extends KeyedProcessFunction<Long,OrderEvent, OrderResult> {

        //定义状态
        private ValueState<Boolean> isCreateState; //作用保证创建订单的动作先出现
        private ValueState<Long> tsState;

        @Override
        public void open(Configuration parameters) throws Exception {
            isCreateState = getRuntimeContext().getState(new ValueStateDescriptor<Boolean>("is-created", Boolean.class));
            tsState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("ts-state", Long.class));
        }


        @Override
        public void processElement(OrderEvent orderEvent, Context context, Collector<OrderResult> collector) throws Exception {
            if("create".equals(orderEvent.getEventType())){
                isCreateState.update(true);

                //注册定时器
                long ts = (orderEvent.getEventTime() + 900) * 1000L;
                context.timerService().registerEventTimeTimer(ts);
                //更新时间状态
                tsState.update(ts);
            }else if("pay".equals(orderEvent.getEventType())){
                if(isCreateState.value()!=null){
                    //正常支付订单
                    collector.collect(new OrderResult(orderEvent.getOrderId(),"normal payed"));
                    //删除定时器
                    context.timerService().deleteEventTimeTimer(tsState.value());
                    //删除该组无效状态
                    isCreateState.clear();
                    tsState.clear();
                }else{
                    //正常生产环境不会有这种情况
                }
            }
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<OrderResult> out) throws Exception {
            //说明超时订单
            //写入侧输出流中
            ctx.output(new OutputTag<OrderResult>("payTimeOut"){},new OrderResult(ctx.getCurrentKey(),"payTimeOut"));
            //清除无效状态
            isCreateState.clear();
            tsState.clear();
        }
    }
}
