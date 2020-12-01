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

/**
 * @version 1.0
 * @description:
 * @author: 宇文智
 * @date 2020/11/30 15:39
 */
//恶意登录监控
    //如果同一用户（可以是不同IP）在2秒之内连续两次登录失败，就认为存在恶意登录的风险
    //实现方式一： 使用定时器，在定时器中判断是否有超过两次登录失败行为
    //这种实现方式会有问题
    // --如果两秒内连续登录超过两次，最后一秒登录成功，会删除当前的定时器，不会出现警报信息
public class LoginFailApp {
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
        private ValueState<Long> timertsState;

        @Override
        public void open(Configuration parameters) throws Exception {
            failLoginListState = getRuntimeContext().getListState(
                    new ListStateDescriptor<LoginEvent>("fail-login-list",LoginEvent.class));
            timertsState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("timer-ts-state",Long.class));
        }

        @Override
        public void processElement(LoginEvent loginEvent, Context context, Collector<String> collector) throws Exception {

            Iterable<LoginEvent> loginEventsIter = failLoginListState.get();

            //1. 若第一条数据
            if(!loginEventsIter.iterator().hasNext()){
                //为失败登录
                if("fail".equals(loginEvent.getEventType())){
                    failLoginListState.add(loginEvent);
                }
                //注册两秒后的定时器，定时器内查看登录失败次数
                long ts = (loginEvent.getTimestamp()+5)*1000L;
                System.out.println(ts);
                context.timerService().registerEventTimeTimer(ts);
                //将该定时器定的时间放到状态中
                timertsState.update(ts);
            }else{
                //判断不是第一条记录
                //为失败登录，failLoginList可能大于 2
                if("fail".equals(loginEvent.getEventType())){
                    System.out.println(loginEvent);
                    failLoginListState.add(loginEvent);
                }else{
                    //成功登录，连续失败登录中断，删除定时器
                    //这种实现方式会有问题
                    // --如果两秒内连续登录超过两次，最后一秒登录成功，会删除当前的定时器，不会出现警报信息
                    System.out.println(timertsState.value());
                    context.timerService().deleteProcessingTimeTimer(timertsState.value());

                    //当前的状态失效 清除
                    failLoginListState.clear();
                    timertsState.clear();
                }
            }

        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            //判断失败登录次数
            Iterable<LoginEvent> loginEventsIter = failLoginListState.get();
            ArrayList<LoginEvent> loginEvents = Lists.newArrayList(loginEventsIter.iterator());

            //失败次数 大于等于 2 ，则输出警报信息
            int size = loginEvents.size();
            System.out.println(size);
            if(size >= 2){
                LoginEvent firstFail = loginEvents.get(0);
                LoginEvent lastFail = loginEvents.get(size - 1);
                out.collect(ctx.getCurrentKey() +
                        "在" + firstFail.getTimestamp() +
                        "到" + lastFail.getTimestamp() +
                        "之间登录失败" + size + "次！");
            }

            //状态失效，清除
            failLoginListState.clear();
            timertsState.clear();
        }
    }
}
