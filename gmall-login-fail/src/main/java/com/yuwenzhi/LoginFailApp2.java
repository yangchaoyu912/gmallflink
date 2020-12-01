package com.yuwenzhi;

import bean.LoginEvent;
import org.apache.commons.compress.utils.Lists;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Iterator;

/**
 * @version 1.0
 * @description:
 * @author: 宇文智
 * @date 2020/11/30 15:39
 */
//恶意登录监控
    //如果同一用户（可以是不同IP）在2秒之内连续两次登录失败，就认为存在恶意登录的风险
    //实现方式二： 判断事件时间两秒内，只要出现连续两次失败，就输出警报信息
    //缺点： 实现连续大于两次（8,9）失败，会很麻烦！！
public class LoginFailApp2 {
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

        //3. 按照userId进行分组
        SingleOutputStreamOperator<String> result = loginEventDS.keyBy(LoginEvent::getUserId)
                .process(new LoginFailKeyProcessFunc());

        result.print();

        //启动任务
        env.execute();
    }

    private static class LoginFailKeyProcessFunc extends KeyedProcessFunction<Long,LoginEvent,String> {

        //定义状态
        private ListState<LoginEvent> failLoginListState; //用于存登录失败的事件信息

        @Override
        public void open(Configuration parameters) throws Exception {
            failLoginListState = getRuntimeContext().getListState(
                    new ListStateDescriptor<LoginEvent>("fail-login-list",LoginEvent.class));
        }

        @Override
        public void processElement(LoginEvent loginEvent, Context context, Collector<String> collector) throws Exception {

            //取出登录失败状态
            Iterator<LoginEvent> failLoginEventIter = failLoginListState.get().iterator();

            //当前数据为失败登录
            if ("fail".equals(loginEvent.getEventType())) {
                //查看状态中是否有之前失败登录记录
                if (failLoginEventIter.hasNext()) {
                    //判断两次失败登录数据之间的时间间隔是否在 2 秒内
                    LoginEvent lastFailLoginEvent = failLoginEventIter.next();
                    if(loginEvent.getTimestamp() - lastFailLoginEvent.getTimestamp() <= 2){
                        //输出警告信息
                        collector.collect(context.getCurrentKey() +
                                "在" + lastFailLoginEvent.getTimestamp() +
                                "到" + loginEvent.getTimestamp() +
                                "之间登录失败" + 2 + "次！");
                    }
                    //清除失效状态
                    failLoginListState.clear();
                }
                //添加到登录失败状态列表中
                failLoginListState.add(loginEvent);
            }else{
                //成功，连续失败登录中断
                //清除失效状态
                failLoginListState.clear();
            }

        }
    }
}
