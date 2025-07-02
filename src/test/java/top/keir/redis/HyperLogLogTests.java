package top.keir.redis;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.redisson.api.RHyperLogLog;

import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;

@Slf4j
class HyperLogLogTests extends RedisApplicationTests {

    @Test
    public void test() throws InterruptedException {
        RHyperLogLog<Object> logLog = redissonClient.getHyperLogLog("api:user");
        CountDownLatch latch = new CountDownLatch(2);
        Thread.startVirtualThread(() -> {
            for (int i = 0; i < 100000; i++) {
                logLog.add(UUID.randomUUID().toString());
            }
            latch.countDown();
        });
        AtomicLong counter = new AtomicLong(10000000);
        Thread.startVirtualThread(() -> {
            for (int i = 0; i < 100000; i++) {
                logLog.add("V" + counter.incrementAndGet());
            }
            latch.countDown();
        });
        latch.await();
        log.info("logLog.count() = {}", logLog.count());
    }

}
