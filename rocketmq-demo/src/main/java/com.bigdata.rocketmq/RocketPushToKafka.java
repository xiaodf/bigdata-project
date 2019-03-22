/**
 * Copyright (C) 2015/3/12 - 2015/3/18  iie Group Holding Limited
 * author
 */
package com.bigdata.rocketmq;

import com.alibaba.rocketmq.client.consumer.DefaultMQPushConsumer;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import com.alibaba.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.common.consumer.ConsumeFromWhere;
import com.alibaba.rocketmq.common.message.MessageExt;
import kafka.javaapi.producer.Producer;
import kafka.producer.ProducerConfig;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;


/**
 * 订阅RocketMQ消息，并推送到Kafka。 并发逻辑在该类之上实现
 * 版本说明：在RocketPushToKafka类的基础上添加Mysql 统计功能
 * 通过SQL可以分析消费数据
 * 添加实时性验证功能
 * 增加Ctrl+C处理逻辑
 * ----------------------------------------------
 * 2015/7/29
 * 统计信息修改
 * mysql增加程序运行主机的IP
 * Count条记录对应的总长度，即字节数
 *
 */
public class RocketPushToKafka extends Thread {

    //统计信息日志输出
    //private static Logger logger = Logger.getLogger("Mlog");
    private static Logger logger = Logger.getLogger("rootLogger");
    //调试信息输出
//    private static Logger infoLogger = Logger.getLogger("Tlog");
    //单位时间内处理消息数量
    private AtomicLong counter = new AtomicLong(0L);
    //程序启动到当前时间处理消息数量
//    private AtomicLong totalcounter = new AtomicLong(0L);
    //RocketMq consume
    private DefaultMQPushConsumer consumer;

    //private static final int MILLIS = 86400000;
    //mysql statement
    private static PreparedStatement statement;
    //数据库连接
    private static Connection con;
    //消息从被客户端创建到被Rocfka消费，中间经历的毫秒数
    private static AtomicLong bornGapTimestamp = new AtomicLong(0L);
    //消息从存储到RocketMQ到被Rocfka消费，中间经历的毫秒数
    private static AtomicLong storeGapTimestamp = new AtomicLong(0L);

    // 2015/7/29
    //本机IP
    private static String host;
    //单位时间内处理的消息总量，单位byte（对应count数量）
    public AtomicLong mesbytes = new AtomicLong(0);

    @Deprecated
    public RocketPushToKafka() {
    }

    /**
     * constructor
     *
     * @param topic                         topic name
     * @param partitionNum                  kafka partition number
     * @param group                         group for rocketmq consumer
     * @param rNameSrvAddr                  rocketmq name server address
     * @param brokers                       kafka brokers
     * @param zkConnector                   zookeeper connect address for kafka
     * @param maxThread                     maximum thread number for rocketmq consumer
     * @param minThread                     minimum thread number for rocketmq consumer
     * @param batchsize
     * @param pullBatchSize                 rocketmq pull batch size
     * @param batchNumMessage               kafka batch number
     * @param bufferMaxMs                   kafka buffer maximum ms
     * @param bufferMaxMessages             kafka buffer maximum message ccount
     * @param pullThresholdForQueue         maximum for pull queue
     * @param pullInterval                  pull interval
     * @param adjustThreadPoolNumsThreshold adjust thread pool number threshold
     */
    public RocketPushToKafka(String topic, int partitionNum, String group,
                             String rNameSrvAddr, String brokers, String zkConnector,
                             int maxThread, int minThread, int batchsize, int pullBatchSize,
                             String batchNumMessage, String bufferMaxMs, String bufferMaxMessages,
                             int pullThresholdForQueue, long pullInterval, long adjustThreadPoolNumsThreshold) {
        //kafka 配置文件
        final Properties props = new Properties();
        props.put("zk.connect", zkConnector);
        props.put("serializer.class", "kafka.serializer.DefaultEncoder");
        props.put("metadata.broker.list", brokers);
        props.put("producer.type", "async");
        props.put("batch.num.messages", batchNumMessage);
        props.put("queue.buffering.max.ms", bufferMaxMs);
        props.put("queue.buffering.max.messages", bufferMaxMessages);

        //init consume
        consumer = new DefaultMQPushConsumer(group);
        consumer.setNamesrvAddr(rNameSrvAddr);
        consumer.setConsumeThreadMax(maxThread);
        consumer.setConsumeThreadMin(minThread);
        consumer.setConsumeMessageBatchMaxSize(batchsize);
        consumer.setPullBatchSize(pullBatchSize);
        consumer.setPullThresholdForQueue(pullThresholdForQueue);
        consumer.setPullInterval(pullInterval);
        consumer.setAdjustThreadPoolNumsThreshold(adjustThreadPoolNumsThreshold);
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET);

        logger.info(consumer.getClass() + "consumer inited");
        try {
            consumer.subscribe(topic, "*");
        } catch (MQClientException e) {
            e.printStackTrace();
        }

        final int numF = partitionNum;
        consumer.registerMessageListener(new MessageListenerConcurrently() {
            //init kafka producer
            Producer<Integer, byte[]> producer = new Producer<Integer, byte[]>(
                    new ProducerConfig(props));
            IncrPartitioner incrPartitoner = new IncrPartitioner(numF);

            public ConsumeConcurrentlyStatus consumeMessage(
                    List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
                producer.send(incrPartitoner.getKeyedMsg(msgs));
                bornGapTimestamp.set(System.currentTimeMillis() - msgs.get(0).getBornTimestamp());
                storeGapTimestamp.set(System.currentTimeMillis() - msgs.get(0).getStoreTimestamp());
                counter.addAndGet(msgs.size());
                for (MessageExt me : msgs) {
                    int lenth = me.getBody().length;
                    mesbytes.addAndGet(lenth);
                }
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });
    }

    // to control, separated from structure
    public void start() {
        try {
            consumer.start();
        } catch (MQClientException e) {
            e.printStackTrace();
        }
        System.out.println("Consumer Started.");
    }

    /**
     * counter清零并返回
     *
     * @return 单位时间处理消息数量
     */
    @Deprecated
    public synchronized long getAndReset() {
        long l = counter.get();
        counter.set(0L);
        return l;
    }

    /**
     * totalcount清零并返回
     *
     * @return totalcount
     */
//    public synchronized long getAndResetT() {
//        // 每天零点totalcount清零
//        long current = System.currentTimeMillis();
//        long curr_hour = current / 1000 / 60 / 60;
//        //long cal = calendar.getTimeInMillis();
//        if (curr_hour % 24 == 0) {
//            totalcounter.set(0L);
//        }
//        long l = totalcounter.get();
//        return l;
//    }

    public synchronized long getAndResetMesbyte() {
        long l = mesbytes.get();
        mesbytes.set(0L);
        return l;
    }

    /**
     * main
     *
     * @param args
     */
    public static void main(String args[]) {
        //hook
        Runtime.getRuntime().addShutdownHook(new RocketPushToKafka());
        //加载配置文件
        Properties rocketProp = new Properties();
        try {
            rocketProp.load(new FileInputStream(new File(args[0])));
            // rocketProp.load(new FileInputStream(new File(configPath)));
        } catch (FileNotFoundException e1) {
            e1.printStackTrace();
            logger.info("无法读取rocket.properties配置文件");
        } catch (IOException e1) {
            e1.printStackTrace();
        }

        final String topics[] = rocketProp.getProperty("topics").split(",");
        final String groups[] = rocketProp.getProperty("groups").split(",");
        String[] partitionNums = rocketProp.getProperty("partitionNums").split(
                ",");
        String rNameSrvAddr = rocketProp.getProperty("rocketNameServerDddr");
        String brokers = rocketProp.getProperty("brokers");
        String zkConnector = rocketProp.getProperty("zkConnector");
        int consumeThreadMax = Integer.parseInt(rocketProp
                .getProperty("consumeThreadMax"));
        int consumeThreadMin = Integer.parseInt(rocketProp
                .getProperty("consumeThreadMin"));
        int ConsumeMessageBatchMaxSize = Integer.parseInt(rocketProp
                .getProperty("ConsumeMessageBatchMaxSize"));
        int pullBatchSize = Integer.parseInt(rocketProp
                .getProperty("pullBatchSize"));
        String batchNumMessage = rocketProp.getProperty("batch.num.messages");
        int interval = Integer.parseInt(rocketProp.getProperty("interval"));
        String bufferMaxMs = rocketProp.getProperty("queue.buffering.max.ms");
        String bufferMaxMessages = rocketProp.getProperty("queue.buffering.max.messages");
        int pullThresholdForQueue = Integer.parseInt(rocketProp.getProperty("pullThresholdForQueue"));
        long pullInterval = Long.parseLong(rocketProp.getProperty("pullInterval"));
        long adjustThreadPoolNumsThreshold = Long.parseLong(rocketProp.getProperty("adjustThreadPoolNumsThreshold"));

        String url = rocketProp.getProperty("url");
        String driver = rocketProp.getProperty("driver");
        String username = rocketProp.getProperty("username");
        String password = rocketProp.getProperty("password");
        final String table = rocketProp.getProperty("table");

        //String createTableSql = "create table if not exists "+table+" (id bigint not null primary key auto_increment,topic varchar(60),count bigint,time timestamp,gap bigint)";
        //2015/7/29
        String createTableSql = "create table if not exists " + table + " (id bigint not null primary key auto_increment,topic varchar(60),count bigint,time timestamp,borngap bigint,storegap bigint,host varchar(16),bytes bigint)";
        try {
            Class.forName("com.mysql.jdbc.Driver");
        } catch (ClassNotFoundException e1) {
            e1.printStackTrace();
            System.out.println("加载驱动失败！！！！！！！！！！！！！！！");
        }
        try {
            con = DriverManager.getConnection(url + "user=" + username + "&password=" + password);
            statement = con.prepareStatement(createTableSql);
            statement.execute();

        } catch (SQLException e) {
            e.printStackTrace();
            System.out.println("无法获取数据库连接");
        }

        // create
        final RocketPushToKafka mirrors[] = new RocketPushToKafka[topics.length];

        // get instances
        for (int i = 0; i < mirrors.length; i++) {
            mirrors[i] = new RocketPushToKafka(topics[i],
                    Integer.parseInt(partitionNums[i]), groups[i],
                    rNameSrvAddr, brokers, zkConnector, consumeThreadMax,
                    consumeThreadMin, ConsumeMessageBatchMaxSize, pullBatchSize,
                    batchNumMessage, bufferMaxMs, bufferMaxMessages, pullThresholdForQueue, pullInterval, adjustThreadPoolNumsThreshold);
        }

        // start all
        for (int i = 0; i < mirrors.length; i++) {
            mirrors[i].start();
        }

        // now
        long now = System.currentTimeMillis();
        long start = interval - now % interval;
        final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        //获取本机IP
        try {
            InetAddress address = Inet4Address.getLocalHost();
            host = address.getHostAddress();
        } catch (UnknownHostException e1) {
            System.out.println("获取本地host失败");
            e1.printStackTrace();
        }

        // start timer, after all has started
        final Timer timer = new Timer("Topic_counter");
        // every ten minutes export counter
        timer.scheduleAtFixedRate(new TimerTask() {
            @Override
            public void run() {
                for (int i = 0; i < mirrors.length; i++) {
                    // 会全部从主线程（main）调用，因为Logger为 static
                    logger.info("topic:" + topics[i]
                            + " count：" + mirrors[i].counter.get());
                    try {
                        String currentTime = sdf.format(new Date());
                        //statement.execute("insert into "+table+" (topic,count,time,gap) values ('"+topics[i]+"',"+mirrors[i].getAndReset()+","+"'"+currentTime+"'"+","+bornGapTimestamp+")");
                        // 2015/7/29
                        String insert = "insert into " + table + " (topic,count,time,gap,host,bytes) values ('" + topics[i] + "'," + mirrors[i].counter.getAndSet(0) + "," + "'" + currentTime + "'" + "," + bornGapTimestamp + ",'" + "," + storeGapTimestamp + ",'" + host + "'," + mirrors[i].mesbytes.getAndSet(0) + ")";
//                        logger.info(insert);
                        statement.execute(insert);
                    } catch (SQLException e) {
                        e.printStackTrace();
                    }
                }
            }

        }, start, interval);
    }

    @Override
    public void run() {
        try {
            //先关闭consume
//			consumer.shutdown();
            con.close();
            logger.info("数据库已关闭");
        } catch (SQLException e) {
            logger.info("数据库无法关闭");
            e.printStackTrace();
        }
    }
}
