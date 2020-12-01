package com.yuwenzhi.app;

import com.yuwenzhi.bean.UserBehavior;
import com.yuwenzhi.bean.UvCount;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import redis.clients.jedis.Jedis;

import java.sql.Timestamp;

/**
 * @version 1.0
 * @description:
 * @author: 宇文智
 * @date 2020/11/30 10:31
 */
public class UvCountWithBloomFilterApp {
    //需求：网站独立访客数（UV）的统计: 一个小时内的网站访客量
    public static void main(String[] args) throws Exception {
        //flink 流处理
        //1. 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //1.2 设置并行度
        env.setParallelism(1);
        //1.3 设置时间语义为事件时间
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        //2. 从文件中读取数据转换为javaBean,同时提取事件时间
        SingleOutputStreamOperator<UserBehavior> inputDS = env.readTextFile("input/UserBehavior.csv")
                .map(line -> {
                    String[] split = line.split(",");
                    return new UserBehavior(Long.parseLong(split[0]),
                            Long.parseLong(split[1]),
                            Integer.parseInt(split[2]),
                            split[3],
                            Long.parseLong(split[4]));
                }).assignTimestampsAndWatermarks(
                        //数据中时间是单调递增的，所以不用设置watermark延时，用一个增量watermark即可
                        new AscendingTimestampExtractor<UserBehavior>() {
                    @Override
                    public long extractAscendingTimestamp(UserBehavior userBehavior) {
                        long eventTime = userBehavior.getTimestamp() * 1000L;
                        return eventTime;
                    }
                });
        //每条记录即是一条访问,开窗，聚合统计（简单使用hashset去重）
        SingleOutputStreamOperator<UvCount> oneHourUvCount = inputDS
                .timeWindowAll(Time.hours(1))
                //这里可以使用Trigger(触发器)来 自定义window 何时求值以及何时发送求值结果
                .trigger(new oneDataTrigger())  //来一条数据，计算一次
                //求值逻辑
                .process(new UvCountWithBloomFilterProcWinowFunc());

        oneHourUvCount.print("one hour");


        //启动任务
        env.execute();

    }

    //自定义触发器：每条数据，触发一次计算并发射计算结果
    //T: 输入数据类型 UserBehavior
    //w: 窗口类型
    private static class oneDataTrigger extends Trigger<UserBehavior,TimeWindow>{

        @Override
        //处理每个元素时
        public TriggerResult onElement(UserBehavior userBehavior, long l, TimeWindow timeWindow, TriggerContext triggerContext) throws Exception {
            return TriggerResult.FIRE_AND_PURGE; //先计算和发送计算结果值，然后将窗口内容丢弃
        }

        @Override
        //默认系统时间窗口结束时调用一次
        public TriggerResult onProcessingTime(long l, TimeWindow timeWindow, TriggerContext triggerContext) throws Exception {
            return TriggerResult.CONTINUE;  //什么都不做
        }

        @Override
        //默认事件时间窗口结束时调用一次
        public TriggerResult onEventTime(long l, TimeWindow timeWindow, TriggerContext triggerContext) throws Exception {
            return TriggerResult.CONTINUE;  //什么都不做
        }

        @Override
        //每个窗口结束时，调用一次
        public void clear(TimeWindow timeWindow, TriggerContext triggerContext) throws Exception {

        }
    }

    //自定义布隆过滤器
    public static class MyBloomFilter {
        public MyBloomFilter() {
        }
        //定义布隆过滤器的容量： 需要2的次幂
        private long cap;
        public MyBloomFilter(long cap) {
            this.cap = cap;
        }
        //一层hash，hash碰撞的几率较大（工作中一般定义3层，保证数据散列性）
        public Long hash(String value) {
            int result = 0;
            for (char c : value.toCharArray()) {
                result = result * 31 + c;   //每个字符的ASCII码做运算
            }
            //位与运算，与取模效果一样，且效率比较高
            return result & (cap - 1);
        }
    }

    //使用ProcessAPI的好处： 有生命周期函数，可以连接外部文件系统
    /**
     * 连接redis，保存两种数据：
     * 1. bitMaps(位图) -> userId的 hash值(offset)，
     *      redis中可以使用 getbit(bm,offset)/setbit(bm,offset,true/false) 直接操作数值的bit位
     *      外层 key: String
     *          name: UvBitMaps + "windowEnd"
     * 2. 网站访客累计值
     *      外层 key: hash(key -> value)
     *          name: "UvCount"
     *      内层 key:  windowEnd
     * out: UvCount
     */
    public static class UvCountWithBloomFilterProcWinowFunc extends ProcessAllWindowFunction<UserBehavior, UvCount, TimeWindow> {

        private Jedis jedisCli;   //定义redis客户端连接
        private MyBloomFilter myBloomFilter; //声明布隆过滤器
        private String uvCountRedisKey; //网站访问数 redis key

        @Override
        public void open(Configuration parameters) throws Exception {
            jedisCli = new Jedis("hadoop102", 6379);
            myBloomFilter = new MyBloomFilter(1L << 29);  //2^29 64M
            uvCountRedisKey = "UvCount";
        }

        @Override
        public void process(Context context, Iterable<UserBehavior> iterable, Collector<UvCount> collector) throws Exception {
            //1.获取窗口信息中结束时间，拼接 "bitMap" 成redisKey
            String windowEnd = new Timestamp(context.window().getEnd()).toString();
            String bitMapsRk = "UvBitMap" + windowEnd;

            //2.判断userId在位图中是否存在
            Long offset = myBloomFilter.hash(iterable.iterator().next().getUserId() + "");
            Boolean isExist = jedisCli.getbit(bitMapsRk, offset);

            //3. 不存在，
            if(!isExist){
                //3.1 将该UserId的hash值作为offset, 写入到位图中
                jedisCli.setbit(bitMapsRk,offset,true);
                //3.2 设置内层key，将当前窗口的网站访客量自增 1
                jedisCli.hincrBy(uvCountRedisKey,windowEnd,1);
            }

            //4. 取出网站访客量
            String count = jedisCli.hget(uvCountRedisKey, windowEnd);

            //5. 发送数据
            collector.collect(new UvCount("uv",windowEnd,Long.parseLong(count)));
        }

        @Override
        public void close() throws Exception {
            super.close();
        }
    }
}
