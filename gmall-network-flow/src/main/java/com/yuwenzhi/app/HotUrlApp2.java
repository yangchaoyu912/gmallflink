package com.yuwenzhi.app;

import com.yuwenzhi.bean.ApacheLog;
import com.yuwenzhi.bean.UrlViewCount;
import org.apache.commons.compress.utils.Lists;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.*;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Map;

/**
 * @version 1.0
 * @description:
 * @author: 宇文智
 * @date 2020/11/28 11:14
 */
public class HotUrlApp2 {
    //需求： 求每5秒计算一次10分钟内的热门页面
    public static void main(String[] args) throws Exception {
        //flink 流处理
        //1. 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //1.2 设置并行度
        env.setParallelism(1);
        //1.3 设置时间语义为事件时间
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        //3. 从网络端口中获取数据
        SingleOutputStreamOperator<ApacheLog> apacheLogDS = env.socketTextStream("hadoop102", 8888).map(line -> {
            String[] fields = line.split(" ");
            SimpleDateFormat sdf = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss");
            long time = sdf.parse(fields[3]).getTime();
            return new ApacheLog(fields[0], fields[1], time, fields[5], fields[6]);
        }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<ApacheLog>(Time.seconds(1)) {
            //因为数据源的事件时间可能是无序的，需要使用watermark对数据进行时间标记 延时 1 秒
            @Override
            public long extractTimestamp(ApacheLog apacheLog) {
                return apacheLog.getEventTime(); //提取事件时间
            }
        });
        //定义侧输出流标签
        OutputTag<ApacheLog> sideOutput = new OutputTag<ApacheLog>("sideOutput"){};

        //4. 过滤Get请求数据，以url分组，开窗，累加计算
        SingleOutputStreamOperator<UrlViewCount> urlViewCountStream = apacheLogDS
                .filter(log -> "GET".equals(log.getMethod()))
                .keyBy(ApacheLog::getUrl)
                //开滑动窗口  窗口大小10分钟 计算时间每5秒
                .timeWindow(Time.minutes(10), Time.seconds(5))
                //允许数据迟到一分钟
                .allowedLateness(Time.seconds(60))
                //窗口关闭，数据流向侧输出流
                .sideOutputLateData(sideOutput)
                //UrlCountAggFunc函数: 聚合 ，计算组内url次数  ①
                //UrlCountWindowFunc函数： 将 ① 处理后的数据进行数据转换，使数据含有窗口信息，便于之后以时间段分组计算
                .aggregate(new UrlCountAggFunc(), new UrlCountWinowFunc());

        //5. 再以窗口结束时间分组，组内排序，求出该窗口中的热门页面TopN
        SingleOutputStreamOperator<String> result = urlViewCountStream.keyBy(UrlViewCount::getWindowEnd).process(new TopNProcessFunc(5));

        apacheLogDS.print("apacheLogDS");
        urlViewCountStream.print("aggregate");
        result.print("result");
        apacheLogDS.getSideOutput(sideOutput).print("side");
        //启动任务
        env.execute();
    }

    private static class UrlCountAggFunc implements AggregateFunction<ApacheLog, Long,Long> {
        @Override
        //初始化页面次数累加器
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        //每一条记录，代表一次页面浏览
        public Long add(ApacheLog apacheLog, Long acc) {
            //System.out.println(acc);
            return acc + 1L;
        }

        @Override
        //对页面次数累加器，做二次处理
        //这里直接返回
        public Long getResult(Long acc) {
            return acc;
        }

        @Override
        //合并多个累加器，在会话窗口中会调用，当前为滑动窗口，不会调用，不需要实现
        public Long merge(Long a, Long b) {
            return null;
        }
    }

    //结构转换： 使UrlCountAggFunc函数聚合后的结果，具有窗口信息
    // IN : 页面次数 (Long)
    // OUT: 返回值封装到 UrlViewCount (url, windowEnd, count)
    // KEY: 之前以url 分组 (String)
    // W: TimeWindow 时间窗口
    private static class UrlCountWinowFunc implements WindowFunction<Long, UrlViewCount,String, TimeWindow> {

        @Override
        public void apply(String url, TimeWindow timeWindow, Iterable<Long> iterable, Collector<UrlViewCount> collector) throws Exception {
            //封装到UrlViewCount
            collector.collect(new UrlViewCount(url,timeWindow.getEnd(),iterable.iterator().next()));
        }
    }

    //组内排序求topN
    //key : windowEnd (Long)
    //In : UrlViewCount
    //Out : topN 信息 （String)
    private static class TopNProcessFunc extends KeyedProcessFunction<Long,UrlViewCount,String> {

        private Integer topSize;

        //求top i
        public TopNProcessFunc(int i) {
            topSize = i;
        }

        //定义一个集合状态： 存放该窗口中所有数据
        // 如果使用list 当再次触发定时器（windowEnd + 1ms），
        // 还没到结束时间后的60秒（等待迟到数据60s） 状态没有被清空
        // list会有相同url的数据
        // 考虑使用MapState，达到覆盖的作用
        private MapState<String,UrlViewCount> mapState;

        @Override
        public void open(Configuration parameters) throws Exception {
            //初始化状态,每个窗口初始化一次，
            mapState = getRuntimeContext().getMapState(new MapStateDescriptor<String,UrlViewCount>("list-state",String.class,UrlViewCount.class));
        }

        @Override
        //处理组内所有元素
        public void processElement(UrlViewCount uvCnt, Context context, Collector<String> collector) throws Exception {
            //将数据放到集合中
            mapState.put(uvCnt.getUrl(),uvCnt);
            //注册“事件时间”定时器，处理状态中的数据
            context.timerService().registerEventTimeTimer(uvCnt.getWindowEnd() + 1L);
            //因为允许一分钟的迟到数据。应该在窗口关闭后清空，即窗口结束时间 + 60s
            context.timerService().registerEventTimeTimer(uvCnt.getWindowEnd() + 60000L);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {

            if(timestamp == ctx.getCurrentKey() + 60000L){
                //清空状态
                mapState.clear();
                return;
            }

            //取出状态中的所有数据
            Iterator<Map.Entry<String, UrlViewCount>> iterator = mapState.iterator();
            ArrayList<Map.Entry<String, UrlViewCount>> entries = Lists.newArrayList(iterator);

            //按照count排序
            //按照降序
            entries.sort((o1, o2) -> {
                if(o1.getValue().getCount() > o2.getValue().getCount()){
                    return -1;
                }else if(o1.getValue().getCount() < o2.getValue().getCount()){
                    return 1;
                }else {
                    return 0;
                }
            });
            //topN商品信息
            StringBuilder sb = new StringBuilder();
            sb.append("===================\n");
            sb.append("当前窗口结束时间为：").append(new Timestamp(timestamp - 1L)).append("\n");

            //遍历topN 有 设定取出数量则用设定，没有则有多少取多少
            //Math.max(topSize,itemCounts.size())
            for (int i = 0; i < Math.min(topSize, entries.size()); i++) {
                //取出数据
                UrlViewCount itemCount = entries.get(i).getValue();
                sb.append("TOP ").append(i + 1);
                sb.append(" url=").append(itemCount.getUrl());
                sb.append(" 页面热度=").append(itemCount.getCount());
                sb.append("\n");
            }
            sb.append("======================\n\n");

            Thread.sleep(1000);     //睡1s 方便查看打印信息

            //输出数据
            out.collect(sb.toString());

        }
    }
}
