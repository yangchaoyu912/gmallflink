package com.yuwenzhi;

import com.yuwenzhi.bean.ItemCount;
import com.yuwenzhi.bean.UserBehavior;
import org.apache.commons.compress.utils.Lists;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;

/**
 * @version 1.0
 * @description:
 * @author: 宇文智
 * @date 2020/11/27 15:41
 */
public class HotItemApp {
    //实时热门商品统计
    public static void main(String[] args) throws Exception {
        //flink 流处理
        //1. 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //2. 设置并行度
        env.setParallelism(1);
        //设置时间语义为事件时间
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        //从文件读取数据创建数据流并转化为javaBean并提取事件时间
        SingleOutputStreamOperator<UserBehavior> userDS = env.readTextFile("input/UserBehavior.csv").map(line -> {
            String[] fields = line.split(",");
            return new UserBehavior(Long.parseLong(fields[0]),
                    Long.parseLong(fields[1]),
                    Integer.parseInt(fields[2]),
                    fields[3],
                    Long.parseLong(fields[4]));
        }).assignTimestampsAndWatermarks(new AscendingTimestampExtractor<UserBehavior>() {  //生成watermark为数据流添加时间标记
            @Override
            public long extractAscendingTimestamp(UserBehavior userBehavior) {
                return userBehavior.getTimestamp() * 1000L;
            }
        });

        //3. 按照"pv"过滤，按照itemId分组开窗，计算数据
        SingleOutputStreamOperator<ItemCount> itemCountDS = userDS.filter(data -> "pv".equals(data.getBehavior()))
                .keyBy(UserBehavior::getItemId)
                .timeWindow(Time.hours(1), Time.minutes(5))
                //基于事件时间，记录中每5分钟计算一次 记录中的一小时数据
                .aggregate(new ItemCountAggFunc(), new ItemCountWindowFunc());

        //4. 需求： 求每个事件时间段（窗口）中商品热度排名TopN
        // 按窗口结束时间分组，并排名输出
        SingleOutputStreamOperator<String> result = itemCountDS.keyBy("windowEnd")
                .process(new TopNItemIdCountProcessFunc(5));

        result.print();

        //启动任务
        env.execute();
    }

    //定义预聚合函数，每条记录 + 1
    public static class ItemCountAggFunc implements AggregateFunction<UserBehavior, Long, Long> {
        @Override
        //创建累加器，初始为0
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(UserBehavior userBehavior, Long acc) {
            //来一条记录，累加 1
            return acc + 1L;
        }

        @Override
        //可对累计的结果再处理
        public Long getResult(Long acc) {
            //直接返回累计结果
            return acc;
        }

        @Override
        //对次数进行合并
        public Long merge(Long acc, Long acc1) {
            return acc + acc1;
        }
    }

    //自定义窗口函数，预聚合结果会进入这个函数中，以userid 分组。返回ItemCount
    public static class ItemCountWindowFunc implements WindowFunction<Long, ItemCount,Long, TimeWindow> {
        @Override
        public void apply(Long itemId, TimeWindow timeWindow, Iterable<Long> iterable, Collector<ItemCount> collector) throws Exception {

            //出口结束时间 （以时间段分组时用到）
            long windowEnd = timeWindow.getEnd();
            //取出累计次数
            Long count = iterable.iterator().next();
            //返回当前窗口全部数据计算的结果
            collector.collect(new ItemCount(itemId,windowEnd,count));
        }
    }

    //以windowEnd分组（Long） 输入数据（itemCount) 输出（数据信息String）
    private static class TopNItemIdCountProcessFunc extends KeyedProcessFunction<Tuple,ItemCount,String> {

        //TopN属性
        private Integer topSize;

        //取出排名前几的商品
        public TopNItemIdCountProcessFunc(int i) {
            topSize = i;
        }

        //定义集合存放同一个窗口中，多个商品热度信息，状态
        private ListState<ItemCount> listState;

        @Override
        public void open(Configuration parameters) throws Exception {
            //初始化商品热度状态
            listState = getRuntimeContext().getListState(new ListStateDescriptor<ItemCount>("list_state", ItemCount.class));
        }


        @Override
        public void processElement(ItemCount itemCount, Context context, Collector<String> collector) throws Exception {
            //进入该时间段的每条数据，都添加到listState中
            listState.add(itemCount);
            //注册定时器，当事件时间超过该窗口结束的 1 毫秒触发定时器
            context.timerService().registerEventTimeTimer(itemCount.getWindowEnd()+1L);
        }

        @Override
        //定时器处理逻辑:  排序打印TopN 商品信息
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            //取出状态中的所有数据
            Iterator<ItemCount> iterator = listState.get().iterator();
            ArrayList<ItemCount> itemCounts = Lists.newArrayList(iterator);

            //按照count排序
            itemCounts.sort(new Comparator<ItemCount>() {
                @Override
                //按照降序
                public int compare(ItemCount o1, ItemCount o2) {
                    if(o1.getCount() > o2.getCount()){
                        return -1; //
                    }else if(o1.getCount() < o2.getCount()){
                        return 1;
                    }else {
                        return 0;
                    }
                }
            });
            //topN商品信息
            StringBuilder sb = new StringBuilder();
            sb.append("===================\n");
            sb.append("当前窗口结束时间为：").append(new Timestamp(timestamp - 1L)).append("\n");

            //遍历topN 有 设定取出数量则用设定，没有则有多少取多少
            //Math.max(topSize,itemCounts.size())
            for (int i = 0; i < Math.min(topSize, itemCounts.size()); i++) {
                //取出数据
                ItemCount itemCount = itemCounts.get(i);
                sb.append("TOP ").append(i + 1);
                sb.append(" ItemId=").append(itemCount.getItemId());
                sb.append(" 商品热度=").append(itemCount.getCount());
                sb.append("\n");
            }
            sb.append("======================\n\n");


            //清空状态，便于记录下个窗口
            listState.clear();

            Thread.sleep(1000);     //睡1s 方便查看打印信息

            //输出数据
            out.collect(sb.toString());

        }

    }
}
