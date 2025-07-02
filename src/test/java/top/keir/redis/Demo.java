package top.keir.redis;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;

@Slf4j
public class Demo {

    @Test
    public void test() throws InterruptedException, ExecutionException {
        FutureTask<Integer> task0 = new FutureTask<>(() -> {
            log.info("开始执行");
            Thread.sleep(1000);
            log.info("执行完毕");
            return 1;
        });
        FutureTask<Integer> task1 = new FutureTask<>(() -> {
            log.info("开始执行");
            Thread.sleep(1000);
            log.info("执行完毕");
            return 1;
        });
        Thread.startVirtualThread(task0);
        Thread.startVirtualThread(task1);

        System.out.println("task1.get() = " + task1.get());
        System.out.println("task0.get() = " + task0.get());
        System.out.println(" = ");
    }

}
