package top.keir.redis;

import lombok.extern.slf4j.Slf4j;
import org.dromara.hutool.core.collection.CollUtil;
import org.junit.jupiter.api.Test;
import org.redisson.api.RStream;
import org.redisson.api.StreamGroup;
import org.redisson.api.StreamMessageId;
import org.redisson.api.stream.StreamAddArgs;
import org.redisson.api.stream.StreamCreateGroupArgs;
import org.redisson.api.stream.StreamReadGroupArgs;
import org.redisson.client.codec.StringCodec;

import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * @see StreamMessageId#NEVER_DELIVERED 从下一个未消费的消息开始,
 * 其他值，是从pending-list中获取已消费未确认的消息
 */
@Slf4j
public class StreamTests extends RedisApplicationTests {
    String g1 = "g01";
    String c1 = "c01";
    String c2 = "c02";
    String s1 = "s01";

    private RStream<String, String> getStream() {
        return redissonClient.getStream(s1, StringCodec.INSTANCE);
    }

    /**
     * 添加数据
     */
    @Test
    void addMessage() {
        for (int i = 0; i < 5; i++) {
            StreamAddArgs<String, String> entry = StreamAddArgs.entries("a", "a" + i, "b", "b" + i);
            getStream().add(entry);
        }
    }

    /**
     * 创建消费组
     */
    @Test
    void createGroup() {
        List<StreamGroup> groups = getStream().listGroups();
        if (groups.stream().anyMatch(group -> group.getName().equals(g1))) {
            return;
        }
        StreamCreateGroupArgs groupArgs = StreamCreateGroupArgs.name(g1);
        groupArgs.id(StreamMessageId.ALL);
        getStream().createGroup(groupArgs);
    }

    @Test
    void consumerNoAck() {
        // 标识从下一个未被消费的消息开始
        StreamReadGroupArgs readGroupArgs = StreamReadGroupArgs.neverDelivered();
        readGroupArgs.count(1);
        // 读取未确认的消息
        Map<StreamMessageId, Map<String, String>> idMapMap;
        String consumerName = c1;
        while (CollUtil.isNotEmpty(idMapMap = getStream().readGroup(g1, consumerName, readGroupArgs))) {
            consumerName = Objects.equals(consumerName, c1) ? c2 : c1;
            for (Map.Entry<StreamMessageId, Map<String, String>> entry : idMapMap.entrySet()) {
                printlnLog(entry);
            }
        }
    }

    @Test
    void consumerAck() {
        // 标识从下一个未被消费的消息开始
        StreamReadGroupArgs readGroupArgs = StreamReadGroupArgs.neverDelivered();
        readGroupArgs.count(1);
        String consumerName = c1;
        // 读取未确认的消息
        Map<StreamMessageId, Map<String, String>> idMapMap;
        while (CollUtil.isNotEmpty(idMapMap = getStream().readGroup(g1, consumerName, readGroupArgs))) {
            consumerName = Objects.equals(consumerName, c1) ? c2 : c1;
            for (Map.Entry<StreamMessageId, Map<String, String>> entry : idMapMap.entrySet()) {
                printlnLog(entry);
                getStream().ack(g1, entry.getKey());
                getStream().remove(entry.getKey());
            }
        }
    }

    @Test
    void consumerPendingAck() {
        // 标识从下一个未被消费的消息开始
        StreamReadGroupArgs readGroupArgs = StreamReadGroupArgs.greaterThan(StreamMessageId.ALL);
        readGroupArgs.count(1);
        // 读取未确认的消息
        Map<StreamMessageId, Map<String, String>> idMapMap;
        while (CollUtil.isNotEmpty(idMapMap = getStream().readGroup(g1, c1, readGroupArgs))
                || CollUtil.isNotEmpty(idMapMap = getStream().readGroup(g1, c2, readGroupArgs))) {
            for (Map.Entry<StreamMessageId, Map<String, String>> entry : idMapMap.entrySet()) {
                printlnLog(entry);
                getStream().ack(g1, entry.getKey());
                getStream().remove(entry.getKey());
            }
        }
    }

    @Test
    void consumerPendingNoAck() {
        // 标识从下一个未被消费的消息开始
        StreamReadGroupArgs readGroupArgs = StreamReadGroupArgs.greaterThan(StreamMessageId.ALL);
        readGroupArgs.count(1);
        // 读取未确认的消息
        Map<StreamMessageId, Map<String, String>> idMapMap;
        while (CollUtil.isNotEmpty(idMapMap = getStream().readGroup(g1, c1, readGroupArgs))
                || CollUtil.isNotEmpty(idMapMap = getStream().readGroup(g1, c2, readGroupArgs))) {
            for (Map.Entry<StreamMessageId, Map<String, String>> entry : idMapMap.entrySet()) {
                printlnLog(entry);
            }
        }
    }

    private void printlnLog(Map.Entry<StreamMessageId, Map<String, String>> entry) {
        log.info("readGroup: id = {}, value = {}", entry.getKey(), entry.getValue());
    }

}
