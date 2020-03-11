package com.jthinking.util.file;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.concurrent.ConcurrentLinkedDeque;

public abstract class CacheQueueListener {

    private static final Logger LOGGER = LoggerFactory.getLogger(CacheQueueListener.class);

    /**
     * 监听开启状态
     */
    private volatile boolean logListenFlag = true;
    private volatile boolean queueSizeCheckFlag = true;

    /**
     * 缓存队列
     */
    private final ConcurrentLinkedDeque<String> LOG_CACHE = new ConcurrentLinkedDeque<>();

    /**
     * 默认缓存队列最大个数
     */
    private static final int DEFAULT_CACHE_QUEUE_SIZE = 20000;

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
    private FileSniffer.CacheQueueFullListener cacheQueueFullListener = (policy, line) -> {
        LOGGER.info("Listener CacheQueueFull Policy: {} Data: {}", policy, line);
    };

    /**
     * 组ID
     */
    private final String groupId;

    /**
     * 监听者ID
     */
    private final String listenerId;


    public CacheQueueListener(String groupId, String listenerId) {
        this.groupId = groupId;
        this.listenerId = listenerId;
        start();
    }

    public static CacheQueueListener of(String groupId, String listenerId) {
        return new CacheQueueListener(groupId, listenerId) {
            @Override
            public void process(String newLine) { }
        };
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
            LOGGER.info("Listener QueueSizeCheck thread {} exit!", Thread.currentThread().getId());
        }).start();
    }

    /**
     * 启动日志监听
     */
    private void startLogListen() {
        new Thread(() -> {
            while (logListenFlag) {
                try {
                    String log = LOG_CACHE.poll();
                    if (log != null) {
                        process(log);
                    } else {
                        try {
                            Thread.sleep(100);
                        } catch (InterruptedException e) {
                            LOGGER.error("", e);
                        }
                    }
                } catch (Exception e) {
                    LOGGER.error("", e);
                }
            }
            LOGGER.info("Listener LogListen thread {} exit!", Thread.currentThread().getId());
        }).start();
    }

    /**
     * 启动
     */
    private void start() {
        startLogListen();
        startQueueSizeCheck();
    }

    /**
     * 停止
     */
    public void stop() {
        logListenFlag = false;
        queueSizeCheckFlag = false;
    }

    public void listen(String newLine) {
        LOG_CACHE.add(newLine);
    }

    public abstract void process(String newLine);

    public String getGroupId() {
        return this.groupId;
    }

    public String getListenerId() {
        return this.listenerId;
    }

    public int getCacheQueueSize() {
        return cacheQueueSize;
    }

    public void setCacheQueueSize(int cacheQueueSize) {
        this.cacheQueueSize = cacheQueueSize;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CacheQueueListener that = (CacheQueueListener) o;
        return groupId.equals(that.groupId) &&
                listenerId.equals(that.listenerId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(groupId, listenerId);
    }
}