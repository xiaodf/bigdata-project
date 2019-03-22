package com.bigdata.rocketmq;

import com.alibaba.rocketmq.common.message.MessageExt;
import kafka.producer.KeyedMessage;

import java.util.ArrayList;
import java.util.List;

public class IncrPartitioner {
    // TODO 保存分区信息 partitioner
    private Integer currentPart = 0;
    private Integer partNum = 1;

    public IncrPartitioner(int partNum){
        this.partNum = partNum;
    }
    /**
     * 设置分区的数量，用于计算msg的分区信息。
     * @param partNum
     */
    public void setPartitionNum(int partNum){
        this.partNum = partNum;
    }

    /**
     * 计算分区信息，哈希取模。
     * @return
     */
    private int getPartKey(){
        currentPart ++;
        currentPart = currentPart % partNum;
        return currentPart;
    }

    /**
     * 将rocketMQ消息列表转换为kafka消息列表
     * @param msgs rocketMQ消息列表
     * @return
     */
    public List<KeyedMessage<Integer, byte[]>> getKeyedMsg(List<MessageExt> msgs) {
        List<KeyedMessage<Integer, byte[]>> kafkaMsgList = new ArrayList<KeyedMessage<Integer, byte[]>>();
        for(MessageExt msg: msgs){
        	//XXXXXXXXXXXX null---->msg.getQueueId() XXXXXXXXXXXX
            kafkaMsgList.add(new KeyedMessage<Integer, byte[]>(msg.getTopic(),null,getPartKey(),msg.getBody()));

        }
        return kafkaMsgList;
    }
}