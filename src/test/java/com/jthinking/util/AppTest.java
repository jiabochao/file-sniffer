package com.jthinking.util;

import com.jthinking.util.file.CacheQueueFullPolicy;
import com.jthinking.util.file.CacheQueueListener;
import com.jthinking.util.file.FileSniffer;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.concurrent.atomic.AtomicLong;


public class AppTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(AppTest.class);
    //@Test
    public void testFileSniffer() throws Exception {

        // 仿Kafka实现
        FileSniffer fs = new FileSniffer(new File("test.log"));
        fs.setCacheQueueSize(10000); // 单位条
        fs.setCacheQueueFullPolicy(CacheQueueFullPolicy.DELETE_OLD);

        fs.setCacheQueueFullListener( (policy, line) -> {
            // 拒绝策略丢弃的数据在这里输出
            LOGGER.info("丢弃 {} {}", policy, line);
        } );

        AtomicLong iCount = new AtomicLong(0);
        CacheQueueListener pushListener = new CacheQueueListener("group-id-1", "listener-id-1") {
            @Override
            public void process(String newLine) {
                // 直接推送
                //LOGGER.info("直接推送 {}", newLine);
                long l = iCount.addAndGet(1);
                if (l > 2100000) {
                    System.out.println(l);
                }
            }
        };
        fs.addCacheQueueListener(pushListener);

        AtomicLong jCount = new AtomicLong(0);
        fs.addCacheQueueListener(new CacheQueueListener("group-id-2", "listener-id-2") {
            @Override
            public void process(String newLine) {
                // 分析
                //LOGGER.info("分析 {}", newLine);
                long l = jCount.addAndGet(1);
                if (l > 2100000) {
                    System.out.println(l);
                }
            }
        });

        //fs.deleteCacheQueueListener(CacheQueueListener.of("group-id", "listener-id"));
        //fs.deleteCacheQueueListener(pushListener);

        fs.start();

        // 阻止程序退出

        Thread.sleep(120 * 1000);

        LOGGER.info("iCount {}", iCount.get());
        LOGGER.info("jCount {}", jCount.get());

        fs.close();

        Thread.sleep(10 * 1000);


    }


}
