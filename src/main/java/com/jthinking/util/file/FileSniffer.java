package com.jthinking.util.file;

import org.apache.commons.io.input.Tailer;
import org.apache.commons.io.input.TailerListenerAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.CountDownLatch;

public class FileSniffer implements Closeable {

    private static final Logger LOGGER = LoggerFactory.getLogger(FileSniffer.class);

    /**
     * 默认缓存队列最大个数
     */
    private static final int DEFAULT_CACHE_QUEUE_SIZE = 20000;

    /**
     * 缓存队列
     */
    private final ConcurrentLinkedDeque<String> LOG_CACHE = new ConcurrentLinkedDeque<>();

    /**
     * 监听文件
     */
    private final File logFile;

    /**
     * 缓存队列最大个数
     */
    private volatile int cacheQueueSize = DEFAULT_CACHE_QUEUE_SIZE;

    /**
     * 缓存队列超过最大个数后清理策略。默认删除旧数据
     */
    private CacheQueueFullPolicy cacheQueueFullPolicy = CacheQueueFullPolicy.DELETE_OLD;

    /**
     * 缓存队列超过最大个数后清理策略监听器。默认打印到日志
     */
    private CacheQueueFullListener cacheQueueFullListener = (policy, line) -> {
        LOGGER.info("FileSniffer CacheQueueFull Policy: {} Data: {}", policy, line);
    };

    /**
     * 已注册的监听器，按group-id分组
     */
    private Map<String, Set<CacheQueueListener>> listenerMap = new HashMap<>();

    /**
     * 文件数据追加读取
     */
    private Tailer tailer;

    /**
     * 日志监听线程退出标识
     */
    private volatile boolean logListenFlag = true;
    private volatile boolean queueSizeCheckFlag = true;

    /**
     * 阻塞
     */
    private CountDownLatch latch = new CountDownLatch(1);

    {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                close();
            } catch (IOException e) {
                LOGGER.error("", e);
            }
            latch.countDown();
        }));
    }

    public FileSniffer(File logFile) {
        this.logFile = logFile;
    }

    /**
     * 单文件单例模式，所有监听相同文件的实例都是同一实例，当监听新文件时，创建新实例
     * @return
     */
    public static FileSniffer getOrNewInstance(File logFile) {
        //
        return null;
    }

    /**
     * 注册日志监听器
     * @param listener
     */
    public void addCacheQueueListener(CacheQueueListener listener) {
        Set<CacheQueueListener> listenerSet = listenerMap.get(listener.getGroupId());
        if (listenerSet != null) {
            listenerSet.add(listener);
        } else {
            Set<CacheQueueListener> temp = new HashSet<>();
            temp.add(listener);
            listenerMap.put(listener.getGroupId(), temp);
        }
    }

    /**
     * 删除日志监听器
     * @param listener
     */
    public void deleteCacheQueueListener(CacheQueueListener listener) {
        Set<CacheQueueListener> listenerSet = listenerMap.get(listener.getGroupId());
        listenerSet.remove(listener);
    }

    /**
     * 设置缓存队列超过最大个数后清理策略监听器
     * @param cacheQueueFullListener
     */
    public void setCacheQueueFullListener(CacheQueueFullListener cacheQueueFullListener) {
        this.cacheQueueFullListener = cacheQueueFullListener;
    }

    /**
     * 获取缓存队列最大个数
     * @return
     */
    public int getCacheQueueSize() {
        return cacheQueueSize;
    }

    /**
     * 设置缓存队列最大个数
     * @param cacheQueueSize
     */
    public void setCacheQueueSize(int cacheQueueSize) {
        this.cacheQueueSize = cacheQueueSize;
    }

    public CacheQueueFullPolicy getCacheQueueFullPolicy() {
        return cacheQueueFullPolicy;
    }

    public void setCacheQueueFullPolicy(CacheQueueFullPolicy cacheQueueFullPolicy) {
        this.cacheQueueFullPolicy = cacheQueueFullPolicy;
    }

    /**
     * 缓存大小控制，删除老数据
     */
    private void startQueueSizeCheck() {
        new Thread(() -> {
            while (queueSizeCheckFlag) {
                int redundant = LOG_CACHE.size() - cacheQueueSize;
                if (redundant > 0) {
                    if (this.cacheQueueFullPolicy == CacheQueueFullPolicy.IGNORE_NEW) {
                        cacheQueueFullListener.listen(this.cacheQueueFullPolicy, LOG_CACHE.pollLast());
                    } else {
                        cacheQueueFullListener.listen(this.cacheQueueFullPolicy, LOG_CACHE.pollFirst());
                    }
                }
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    LOGGER.error("", e);
                }
            }
            LOGGER.info("FileSniffer QueueSizeCheck thread {} exit!", Thread.currentThread().getId());
        }).start();
    }

    private void startQueueListen() {
        new Thread(() -> {
            while (logListenFlag) {
                try {
                    List<String> batch = new ArrayList<>();
                    int batchSize = Math.min(LOG_CACHE.size(), 1000);
                    if (batchSize == 0) {
                        try {
                            Thread.sleep(1000);
                        } catch (InterruptedException e) {
                            LOGGER.error("", e);
                        }
                        continue;
                    }
                    for (int ii = 0; ii < batchSize; ii++) {
                        String log = LOG_CACHE.poll();
                        if (log != null) {
                            batch.add(log);
                        } else {
                            break;
                        }
                    }
                    if (batch.size() == 0) {
                        continue;
                    }

                    // 分组消费数据
                    for (Map.Entry<String, Set<CacheQueueListener>> entry : listenerMap.entrySet()) {
                        // TODO 线程池实现提高性能
                        new Thread(() -> {
                            // 同一group中的listener
                            String groupId = entry.getKey();
                            List<CacheQueueListener> listeners = new ArrayList<>(entry.getValue());
                            int listenerIndex = 0;

                            for (String log : batch) {
                                CacheQueueListener listener = listeners.get(listenerIndex);
                                listener.listen(log);
                                if (listeners.size() > listenerIndex + 1) {
                                    listenerIndex++;
                                } else {
                                    listenerIndex = 0;
                                }
                            }
                        }).start();
                    }

                } catch (Exception e) {
                    LOGGER.error("", e);
                }
            }
            LOGGER.info("FileSniffer QueueListen thread {} exit!", Thread.currentThread().getId());
        }).start();
    }

    private void startTailer() {
        tailer = new Tailer(logFile, new TailerListenerAdapter() {

            @Override
            public void fileNotFound() {
                LOGGER.error("{} file not found", logFile.getName());
                super.fileNotFound();
            }

            @Override
            public void fileRotated() {
                //文件被外部的输入流改变
                super.fileRotated();
            }

            @Override
            public void handle(String line) {
                //增加的文件的内容
                LOG_CACHE.add(line);
                super.handle(line);
            }

            @Override
            public void handle(Exception ex) {
                LOGGER.error("", ex);
                super.handle(ex);
            }

        }, 1000, true, 4096);

        new Thread(tailer).start();
    }

    /**
     * 启动FileSniffer
     */
    public void start() {
        startTailer();
        startQueueListen();
        startQueueSizeCheck();
    }

    /**
     * 启动FileSniffer并阻塞，直到Ctrl+C退出
     */
    public void startBlockUtilCancel() {
        start();
        try {
            latch.await();
        } catch (InterruptedException e) {
            LOGGER.error("", e);
        }
    }

    /**
     * 关闭FileSniffer
     * @throws IOException
     */
    @Override
    public void close() throws IOException {
        if (tailer != null) {
            tailer.stop();
        }
        logListenFlag = false;
        queueSizeCheckFlag = false;
        for (Map.Entry<String, Set<CacheQueueListener>> entry : listenerMap.entrySet()) {
            for (CacheQueueListener listener : entry.getValue()) {
                listener.stop();
            }
        }
    }


    public interface CacheQueueFullListener {
        void listen(CacheQueueFullPolicy policy, String line);
    }

}
