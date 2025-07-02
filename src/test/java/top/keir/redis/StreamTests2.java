package top.keir.redis;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.redisson.api.PendingResult;
import org.redisson.api.RStream;
import org.redisson.api.StreamConsumer;
import org.redisson.api.StreamGroup;
import org.redisson.api.StreamMessageId;
import org.redisson.api.stream.StreamAddArgs;
import org.redisson.api.stream.StreamCreateGroupArgs;
import org.redisson.api.stream.StreamReadGroupArgs;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;

@Slf4j
public class StreamTests2 extends RedisApplicationTests {

    String consumerGroupName = "consumer-group";
    String consumerName1 = "consumer-1";
    String consumerName2 = "consumer-2";

    @Test
    void test() throws InterruptedException, ExecutionException {
        RStream<Object, Object> stream = redissonClient.getStream("stream:comment");
        // 增加数据
        for (int i = 0; i < 5; i++) {
            StreamAddArgs<Object, Object> entry = StreamAddArgs.entries("a", "a" + i, "b", "b" + i);
            stream.add(entry);
        }

        // 创建消费组
        createGroup(stream);

        // 创建消费者
        createConsumer(stream);

        // 消费
        FutureTask<Boolean> task0 = new FutureTask<>(() -> {
            consumer(stream, consumerName1);
            return true;
        });
        FutureTask<Boolean> task1 = new FutureTask<>(() -> {
            consumer(stream, consumerName2);
            return true;
        });
        Thread.ofVirtual().start(task0);
        Thread.ofVirtual().start(task1);
        task0.get();
        task1.get();
    }


    void consumer(RStream<Object, Object> stream, String consumerName) {
        while (true) {
            StreamReadGroupArgs readGroupArgs = StreamReadGroupArgs.neverDelivered();
            readGroupArgs.count(1);
            // 读取未确认的消息
            Map<StreamMessageId, Map<Object, Object>> idMapMap = stream
                    .readGroup(consumerGroupName, consumerName, readGroupArgs);
            if (readGroupNoAck(stream, idMapMap)) {
                continue;
            }
            PendingResult pendingInfo = stream.getPendingInfo(consumerGroupName);
            long total = pendingInfo.getTotal();
            if (total > 0) {
                // 重复消费问题
                idMapMap = stream.pendingRange(consumerGroupName, StreamMessageId.MIN, StreamMessageId.MAX, 2);
                if (readGroup(stream, idMapMap)) {
                    continue;
                }
            }
            break;
        }
    }

    boolean readGroup(RStream<Object, Object> stream, Map<StreamMessageId, Map<Object, Object>> idMapMap) {
        if (idMapMap.isEmpty()) {
            return false;
        }
        for (Map.Entry<StreamMessageId, Map<Object, Object>> entry : idMapMap.entrySet()) {
            log.info("readGroup: id = {}, value = {}", entry.getKey(), entry.getValue());
            // 消费完要ack
            stream.ack(consumerGroupName, entry.getKey());
            stream.remove(entry.getKey());
        }
        return true;
    }

    boolean readGroupNoAck(RStream<Object, Object> stream, Map<StreamMessageId, Map<Object, Object>> idMapMap) {
        if (idMapMap.isEmpty()) {
            return false;
        }
        for (Map.Entry<StreamMessageId, Map<Object, Object>> entry : idMapMap.entrySet()) {
            log.info("readGroupNoAck: id = {}, value = {}", entry.getKey(), entry.getValue());
            // 消费完要ack
        }
        return true;
    }

    private void createConsumer(RStream<Object, Object> stream) {
        List<StreamConsumer> consumerList = stream.listConsumers(consumerGroupName);
        if (consumerList.stream().noneMatch(consumer -> consumer.getName().equals(consumerName1))) {
            stream.createConsumer(consumerGroupName, consumerName1);
        }
        if (consumerList.stream().noneMatch(consumer -> consumer.getName().equals(consumerName2))) {
            stream.createConsumer(consumerGroupName, consumerName2);
        }
    }

    void createGroup(RStream<Object, Object> stream) {
        List<StreamGroup> groups = stream.listGroups();
        if (groups.stream().anyMatch(group -> group.getName().equals(consumerGroupName))) {
            return;
        }
        StreamCreateGroupArgs groupArgs = StreamCreateGroupArgs.name(consumerGroupName);
        StreamMessageId messageId = new StreamMessageId(0, 0);
        groupArgs.id(messageId);
        stream.createGroup(groupArgs);
    }


}
