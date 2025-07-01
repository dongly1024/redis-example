package top.keir.redis;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.redisson.api.RMap;
import org.redisson.client.codec.IntegerCodec;

@Slf4j
class RedisMapTests extends RedisApplicationTests {

    @Test
    void addAndGet() {
        RMap<String, Integer> map = redissonClient.getMap("map", IntegerCodec.INSTANCE);
        Integer integer = map.addAndGet("10086", 1);
        log.info("integer = {}", integer);
        Integer integer1 = map.addAndGet("10086", 1);
        log.info("integer1 = {}", integer1);
        Integer integer2 = map.addAndGet("10086", 1);
        log.info("integer2 = {}", integer2);
        Integer integer3 = map.addAndGet("10086", -1);
        log.info("integer3 = {}", integer3);
        Integer integer4 = map.get("10086");
        log.info("integer4 = {}", integer4);
    }

}
