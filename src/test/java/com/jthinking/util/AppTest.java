package com.jthinking.util;

import com.jthinking.util.file.CacheQueueFullPolicy;
import com.jthinking.util.file.CacheQueueListener;
import com.jthinking.util.file.FileSniffer;
import org.apache.commons.io.filefilter.WildcardFileFilter;
import org.apache.commons.io.monitor.FileAlterationListenerAdaptor;
import org.apache.commons.io.monitor.FileAlterationMonitor;
import org.apache.commons.io.monitor.FileAlterationObserver;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileFilter;
import java.util.Arrays;
import java.util.Comparator;
import java.util.concurrent.TimeUnit;
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

        //Thread.sleep(120 * 1000);

        LOGGER.info("iCount {}", iCount.get());
        LOGGER.info("jCount {}", jCount.get());

        fs.close();

        //Thread.sleep(10 * 1000);


    }

    @Test
    public void test2() throws Exception {
        // 设置监控目录
        //String monitorDir = "/usr/local/phpstudy/soft/nginx/nginx-1.15/nginx/logs/";
        File monitorDir = new File("C:\\Users\\jiabo\\Desktop\\demo");

        // 创建过滤器
        // FileFilter fileFilter = new WildcardFileFilter("access_*.log");
        FileFilter fileFilter = new WildcardFileFilter("readme*.md");



        File[] files = monitorDir.listFiles(fileFilter);
        if (files != null) {
            for (File file : files) {
                long l = file.lastModified();
                System.out.println(file);
            }
        }

        // 使用过滤器：装配过滤器，生成监听者
        FileAlterationObserver observer = new FileAlterationObserver(monitorDir, fileFilter);
        // 向监听者添加监听器，并注入业务服务
        observer.addListener(new ListenerAdaptor());
        // 创建文件变化监听器
        FileAlterationMonitor fileAlterationMonitor = new FileAlterationMonitor(TimeUnit.SECONDS.toMillis(5), observer);
        try {
            // 开启监听
            fileAlterationMonitor.start();
        } catch (Exception e) {
            e.printStackTrace();
        }

        /*while (true) {
            Thread.sleep(1);
        }*/


    }



    public static class ListenerAdaptor extends FileAlterationListenerAdaptor {

        /**
         * 目录创建
         **/
        @Override
        public void onDirectoryCreate(File directory) {
            System.out.println("创建新目录");
        }

        /**
         * 目录修改
         **/
        @Override
        public void onDirectoryChange(File directory) {
            System.out.println("目录修改");
        }

        /**
         * 目录删除
         **/
        @Override
        public void onDirectoryDelete(File directory) {
            System.out.println("目录删除");
        }

        /**
         * 文件创建（对于文件名的修改，会先触发文件新增方法，再触发文件删除方法）
         **/
        @Override
        public void onFileCreate(File file) {
            System.out.println("文件创建：" + file);
        }

        /**
         * 文件修改
         **/
        @Override
        public void onFileChange(File file) {
            System.out.println("文件修改");
        }

        /**
         * 文件创建删除
         **/
        @Override
        public void onFileDelete(File file) {
            System.out.println("删除文件：" + file);
        }

        /**
         * 扫描开始
         **/
        @Override
        public void onStart(FileAlterationObserver observer) {
            System.out.println("onStart");
        }

        /**
         * 扫描结束
         **/
        @Override
        public void onStop(FileAlterationObserver observer) {
            System.out.println("onStop");
        }
    }

    @Test
    public void test3() {
        String logName = "/";
        int i = logName.lastIndexOf("/");
        String pathname;
        String filename;
        if (i != -1) {
            if (i == 0) {
                pathname = "/";
            } else {
                pathname = logName.substring(0, i);
            }
            if (i + 1 == logName.length()) {
                filename = "*";
            } else {
                filename = logName.substring(i + 1);
            }
        } else {
            pathname = ".";
            filename = logName;
        }
        System.out.println(pathname);
        System.out.println(filename);
    }

    @Test
    public void test4() {
        Long[] files = {5L, 4L, 3L, 2L, 1L};
        Long[] longs = Arrays.stream(files).sorted(Comparator.comparingLong(x -> x)).toArray(Long[]::new);
        System.out.println(Arrays.toString(longs));
    }

    @Test
    public void test5() throws Exception {

        // 仿Kafka实现
        FileSniffer fs = new FileSniffer("C:/Users/jiabo/Desktop/demo/readme*.md");
        fs.setCacheQueueSize(10000); // 单位条
        fs.setCacheQueueFullPolicy(CacheQueueFullPolicy.DELETE_OLD);

        fs.setCacheQueueFullListener( (policy, line) -> {
            // 拒绝策略丢弃的数据在这里输出
            LOGGER.info("丢弃 {} {}", policy, line);
        } );

        CacheQueueListener pushListener = new CacheQueueListener("group-id-1", "listener-id-1") {
            @Override
            public void process(String newLine) {
                // 直接推送
                LOGGER.info("直接推送 {}", newLine);
            }
        };
        fs.addCacheQueueListener(pushListener);

        fs.start();

        // 阻止程序退出

        /*while (true) {
            Thread.sleep(120 * 1000);
        }*/



    }


}
