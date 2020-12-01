package com.yuwenzhi.app;

import com.yuwenzhi.bean.OrderEvent;
import com.yuwenzhi.bean.OrderResult;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.PatternTimeoutFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

import java.util.List;
import java.util.Map;

/**
 * @version 1.0
 * @description:
 * @author: 宇文智
 * @date 2020/11/30 20:34
 */
//订单支付实时监控
    //使用flink 完成本该javaee完成的订单超时15分钟后取消的业务
public class OrderTimeOutAppWithCep {
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

        //3.定义模式序列
        //订单数据，先有创建信息后有支付信息
        Pattern<OrderEvent, OrderEvent> pattern = Pattern.<OrderEvent>begin("start").where(new SimpleCondition<OrderEvent>() {
            @Override
            public boolean filter(OrderEvent orderEvent) throws Exception {
                return "create".equals(orderEvent.getEventType());
            }
        }).followedBy("follow").where(new SimpleCondition<OrderEvent>() {
            @Override
            public boolean filter(OrderEvent orderEvent) throws Exception {
                return "pay".equals(orderEvent.getEventType());
            }
        }).within(Time.minutes(15));//模式序列在15分钟有效，超时的事件另做处理
        //4. 将模式作用在流上
        PatternStream<OrderEvent> ptStream = CEP.pattern(orderEventDS.keyBy(OrderEvent::getOrderId), pattern);

        //5. 选择事件
        OutputTag<OrderResult> timeOutTag = new OutputTag<OrderResult>("timeOut") {
        };//outTag保存超时事件序列
        SingleOutputStreamOperator<OrderResult> result = ptStream.select(timeOutTag,
                new TimeOutSelectFunc(), //超时事件提取
                new NormalPaySelectFunc());//匹配事件提取

        //6. 打印数据
        result.print("normal payed");
        // 超时订单
        DataStream<OrderResult> sideOutput = result.getSideOutput(timeOutTag);
        sideOutput.print("timeOut");


        //启动任务
        env.execute();
    }

    //自定义超时事件的处理逻辑
    private static class TimeOutSelectFunc implements PatternTimeoutFunction<OrderEvent,OrderResult> {

        @Override
        public OrderResult timeout(Map<String, List<OrderEvent>> pattern, long timeoutTimestamp) throws Exception {
            //开始模式肯定会匹配上
            List<OrderEvent> start = pattern.get("start");
            Long timeOutOrderId = start.get(0).getOrderId();
            return new OrderResult(timeOutOrderId,"timeOut" + timeoutTimestamp);
        }
    }

    //自定义匹配事件的处理逻辑
    private static class NormalPaySelectFunc implements PatternSelectFunction<OrderEvent,OrderResult>{
        @Override
        public OrderResult select(Map<String, List<OrderEvent>> pattern) throws Exception {
            List<OrderEvent> start = pattern.get("start");
            return new OrderResult(start.get(0).getOrderId(),"normal payed");
        }
    }
}
