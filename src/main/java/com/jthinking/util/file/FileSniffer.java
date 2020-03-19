package com.jthinking.util.file;

import org.apache.commons.io.filefilter.WildcardFileFilter;
import org.apache.commons.io.input.Tailer;
import org.apache.commons.io.input.TailerListenerAdapter;
import org.apache.commons.io.monitor.FileAlterationListenerAdaptor;
import org.apache.commons.io.monitor.FileAlterationMonitor;
import org.apache.commons.io.monitor.FileAlterationObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

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
    private File logFile;

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
     * Tailer监听队列
     */
    private ConcurrentLinkedDeque<Tailer> tailerList = new ConcurrentLinkedDeque<>();

    /**
     * 文件夹监听
     */
    private FileAlterationMonitor fileAlterationMonitor;

    /**
     * 需要监听的文件夹
     */
    private File monitorDir;

    /**
     * 需要监听的文件夹中的文件过滤通配符
     */
    private FileFilter fileFilter;

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

    @Deprecated
    public FileSniffer(File logFile) {
        this.logFile = logFile;
    }

    /**
     * 日志文件监听支持通配符，支持多个文件，按最后修改时间排序，只监听最后三个文件
     * @param logName
     */
    public FileSniffer(String logName) {
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
        this.monitorDir = new File(pathname);
        this.fileFilter = new WildcardFileFilter(filename);
    }

    /**
     * TODO 单文件单例模式，所有监听相同文件的实例都是同一实例，当监听新文件时，创建新实例
     * @return
     */
    public static FileSniffer getOrNewInstance(File logFile) {
        // TODO
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

    @Deprecated
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

    private void startTailer2() {
        File[] files = monitorDir.listFiles(fileFilter);
        if (files != null) {
            addAndStartTailer(Arrays.stream(files).sorted(Comparator.comparingLong(File::lastModified)).toArray(File[]::new));
        }
        // 使用过滤器：装配过滤器，生成监听者
        FileAlterationObserver observer = new FileAlterationObserver(monitorDir, fileFilter);
        // 向监听者添加监听器，并注入业务服务
        observer.addListener(new FileAlterationListenerAdaptor() {
            @Override
            public void onFileCreate(File file) {
                addAndStartTailer(file);
            }
        });
        // 创建文件变化监听器
        this.fileAlterationMonitor = new FileAlterationMonitor(TimeUnit.SECONDS.toMillis(5), observer);
        // 开启监听
        try {
            this.fileAlterationMonitor.start();
        } catch (Exception e) {
            LOGGER.error("", e);
        }
    }

    private void addAndStartTailer(File... files) {
        for (File file : files) {
            Tailer tailer = new Tailer(file, new TailerListenerAdapter() {
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
            tailerList.add(tailer);
        }
    }

    private void listenTailerQueue() {
        Thread thread = new Thread(() -> {
            while (true) {
                try {
                    int count = tailerList.size() - 3;
                    if (count > 0) {
                        for (int i = 0; i < count; i++) {
                            Tailer take = tailerList.poll();
                            if (take == null) {
                                try {
                                    Thread.sleep(1000);
                                } catch (InterruptedException e) {
                                    LOGGER.error("", e);
                                }
                                continue;
                            }
                            take.stop();
                        }
                    } else {
                        try {
                            Thread.sleep(1000);
                        } catch (InterruptedException e) {
                            LOGGER.error("", e);
                        }
                    }
                } catch (Exception e) {
                    LOGGER.error("", e);
                }
            }
        });
        thread.setDaemon(true);
        thread.start();
    }

    /**
     * 启动FileSniffer
     */
    public void start() {
        listenTailerQueue();
        startTailer2();
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
        if (fileAlterationMonitor != null) {
            try {
                fileAlterationMonitor.stop();
            } catch (Exception e) {
                LOGGER.error("", e);
            }
        }
        if (tailer != null) {
            tailer.stop();
        }
        for (Tailer t : tailerList) {
            t.stop();
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
