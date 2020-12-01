package com.yuwenzhi.app;

import com.yuwenzhi.bean.AdClickEvent;
import com.yuwenzhi.bean.AdCountByProvince;
import com.yuwenzhi.bean.BlackListWarning;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.sql.Timestamp;

/**
 * @version 1.0
 * @description:
 * @author: 宇文智
 * @date 2020/11/30 13:22
 */
//市场营销商业指标统计分析
// --按照省份划分，统计广告点击量 (若单日某个用户的广告的点击量超过100次，则加入黑名单)
public class AdClickByProvinceApp {
    public static void main(String[] args) throws Exception {
        //flink 流处理
        //1. 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //1.2 设置并行度
        env.setParallelism(1);
        //1.3 设置时间语义为事件时间
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        //2.读取文本数据创建流转换为JavaBean,并指定时间戳字段
        SingleOutputStreamOperator<AdClickEvent> adClickDS = env.readTextFile("input/AdClickLog.csv")
                .map(line -> {
                    String[] fields = line.split(",");
                    return new AdClickEvent(Long.parseLong(fields[0]),
                            Long.parseLong(fields[1]),
                            fields[2],
                            fields[3],
                            Long.parseLong(fields[4]));
                })//数据事件时间有序单调递增，无需设置水位线延迟 watermark间隔（默认为200毫秒）
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<AdClickEvent>() {
                    @Override
                    public long extractAscendingTimestamp(AdClickEvent element) {
                        return element.getTimestamp() * 1000L;
                    }
                });

        //3. 数据过滤： 单日某个用户点击某个广告次数超过100，加入黑名单并将该条数据过滤掉
        //按照userId 和 adId分组，过滤掉超过100次的数据
        SingleOutputStreamOperator<AdClickEvent> filterStream = adClickDS.keyBy("userId", "adId")
                .process(new AdClickProcessFunc(100L));
        filterStream.print("filter");

        //4. 计算各个省份的广告点击量，5秒计算一次 1 个小时的数据
        SingleOutputStreamOperator<AdCountByProvince> result = filterStream.keyBy(AdClickEvent::getProvince)
                .timeWindow(Time.hours(1), Time.seconds(5))
                //来一条计算一条
                .aggregate(new AdClickCountAggFunc(), new AdClickWindowFunc());

        //5. 从侧输出流中获取警告信息
        DataStream<BlackListWarning> sideOutput = filterStream.getSideOutput(new OutputTag<BlackListWarning>("outPut") {
        });
        sideOutput.print("sideOutput");

        //6. 打印结果
        result.print("result");

        //启动任务
        env.execute();
    }

    //处理根据userId 和 AdId分组后的数据，每组数据会进入到这个Process方法中处理
    //该函数中的状态，每组都会有一份
    private static class AdClickProcessFunc extends KeyedProcessFunction<Tuple,AdClickEvent,AdClickEvent> {

        //定义单日某人点击某个广告的上限
        private Long maxClick;

        public AdClickProcessFunc(Long i) {
            this.maxClick = i;
        }

        //定义状态：     需要对点击次数做累计 => 增量状态 ，
        //      判断是否是超过100次的组内黑名单成员 => 布尔类型状态
        private ValueState<Long> adClickCountState;
        private ValueState<Boolean> isBlackListMemberState;


        @Override
        public void open(Configuration parameters) throws Exception {
            //全过程只调用一次-
            adClickCountState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("ad-click-count",Long.class));
            isBlackListMemberState = getRuntimeContext().getState(new ValueStateDescriptor<Boolean>("is-blackList-member",Boolean.class));
        }

        @Override
        //处理组内每个元素
        public void processElement(AdClickEvent adClickEvent, Context context, Collector<AdClickEvent> collector) throws Exception {
            //1. 取出状态中的该组的广告点击量
            //第一条数据，adClickCount,isBlackListMember = null
            Long adClickCount = adClickCountState.value();
            Boolean isBlackListMember = isBlackListMemberState.value();

            //2. 判断是否为第一条数据
            if(adClickCount == null){
                adClickCountState.update(1L);

                //注册定时器，用于第二日清空状态(这里的时区东八区，需减八小时)
                long ts = (adClickEvent.getTimestamp() / (60 * 60 * 24) + 1) * (24 * 60 * 60 * 1000L) - (8 * 60 * 60 * 1000L);
                System.out.println(new Timestamp(ts));
                context.timerService().registerEventTimeTimer(ts);
            }else{
                long currentCount = adClickCount + 1L;
                adClickCountState.update(currentCount);
                //先判断是否被拉黑了
                if(isBlackListMember!=null && isBlackListMember){
                   //直接返回，将这条数据丢弃
                    return;
                }else {
                    //达到单日点击次数
                    if (currentCount == maxClick) {
                        System.out.println("-----------------------------");
                        //拉黑
                        isBlackListMemberState.update(true);
                        //将警告信息从侧输出流中输出
                        context.output(new OutputTag<BlackListWarning>("outPut") {
                                       },
                                new BlackListWarning(adClickEvent.getUserId(), adClickEvent.getAdId(), "点击次数超过" + maxClick + "次！"));
                    }
                }
            }
            // 将合格的数据输出
            collector.collect(adClickEvent);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<AdClickEvent> out) throws Exception {
            adClickCountState.clear();
            isBlackListMemberState.clear();
        }
    }

    private static class AdClickCountAggFunc implements AggregateFunction<AdClickEvent,Long,Long> {
        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(AdClickEvent adClickEvent, Long acc) {
            return acc + 1L;
        }

        @Override
        public Long getResult(Long aLong) {
            return aLong;
        }

        @Override
        public Long merge(Long aLong, Long acc1) {
            return null;
        }
    }

    //输出聚合后的数据
    private static class AdClickWindowFunc implements WindowFunction<Long, AdCountByProvince,String, TimeWindow> {
        @Override
        public void apply(String province, TimeWindow timeWindow, Iterable<Long> iterable, Collector<AdCountByProvince> collector) throws Exception {
            collector.collect(new AdCountByProvince(province,
                    new Timestamp(timeWindow.getEnd()).toString(),
                    iterable.iterator().next()));
        }
    }
}
