package top.keir.redis;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.redisson.api.RMap;
import org.redisson.client.codec.LongCodec;

import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
class RedisMapTests extends RedisApplicationTests {

    @Test
    void addAndGet() throws InterruptedException {
        RMap<String, Long> map = redissonClient.getMap("map", LongCodec.INSTANCE);
        System.out.println("map.size() = " + map.size());
        AtomicInteger counter = new AtomicInteger(10000);
        Thread.startVirtualThread(() -> {
            log.info("11111111");
        }).join();

    }

}
