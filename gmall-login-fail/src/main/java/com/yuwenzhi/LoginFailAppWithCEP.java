package com.yuwenzhi;

import bean.LoginEvent;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * @version 1.0
 * @description:
 * @author: 宇文智
 * @date 2020/11/30 15:39
 */
//恶意登录监控
    //如果同一用户（可以是不同IP）在2秒之内连续两次登录失败，就认为存在恶意登录的风险
    //实现方式三： 使用CEP 进行复杂事件处理
public class LoginFailAppWithCEP {
    public static void main(String[] args) throws Exception {
        //flink 流处理
        //1. 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //1.2 设置并行度
        env.setParallelism(1);
        //1.3 设置时间语义为事件时间
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        //2.读取文本数据创建流,转换为JavaBean,提取时间戳
        SingleOutputStreamOperator<LoginEvent> loginEventDS = env.readTextFile("input/LoginLog.csv")
                .map(line -> {
                    String[] fields = line.split(",");
                    return new LoginEvent(Long.parseLong(fields[0]),
                            fields[1],
                            fields[2],
                            Long.parseLong(fields[3]));
                }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<LoginEvent>(Time.seconds(2)) {
                    @Override
                    public long extractTimestamp(LoginEvent element) {
                        return element.getTimestamp() * 1000L;
                    }
                });

        //3. 使用Cep类库
        //3.1 定义模式序列 使用next().where() 个体模式，默认是严格近邻
//        Pattern<LoginEvent, LoginEvent> pattern = Pattern.<LoginEvent>begin("start").where(new SimpleCondition<LoginEvent>() {
//            @Override
//            public boolean filter(LoginEvent loginEvent) throws Exception {
//                return "fail".equals(loginEvent.getEventType());
//            }
//        }).next("next").where(new SimpleCondition<LoginEvent>() {
//            @Override
//            public boolean filter(LoginEvent loginEvent) throws Exception {
//                return "fail".equals(loginEvent.getEventType());
//            }
//        }).within(Time.seconds(2));  //超时事件序列（部分事件序列可能因为超过窗口长度而被丢弃；为了能够处理这些超时的部分匹配）
        //3.2 使用循环模式 ，默认是宽松近邻
        Pattern<LoginEvent, LoginEvent> pattern = Pattern.<LoginEvent>begin("start").where(new SimpleCondition<LoginEvent>() {
            @Override
            public boolean filter(LoginEvent loginEvent) throws Exception {
                return "fail".equals(loginEvent.getEventType());
            }
        }).times(2).consecutive() //设置严格近邻
                .within(Time.seconds(2));


        //4. 将模式序列（规则）定义在分组后流上
        PatternStream<LoginEvent> patternStream = CEP.pattern(loginEventDS.keyBy(LoginEvent::getUserId), pattern);

        //5. 从检测到事件序列中提取事件
        SingleOutputStreamOperator<String> result = patternStream.select(new LoginFailSelectFunc());


        result.print();

        //启动任务
        env.execute();
    }

    private static class LoginFailSelectFunc implements PatternSelectFunction<LoginEvent,String> {

        @Override
        public String select(Map<String, List<LoginEvent>> pattern) throws Exception {
            List<LoginEvent> start = pattern.get("start");//获取该个体模式检测到的事件
//            List<LoginEvent> next = pattern.get("next");
            //返回警告信息
            return "在" + start.get(0).getTimestamp() +
                    "到" + start.get(1).getTimestamp() + " " +
                    start.get(0).getUserId() +
                    "之间登录失败2次！";
        }
    }
}
