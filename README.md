# file-sniffer

## 介绍

文件内容追加监听。通过监听单个文件变动，读取新增行，并将新增数据分流处理。

文件监听功能使用`commons-io`的`Tailer`实现，数据分流功能仿照`Kafka`中的`Group`概念。

## 快速开始

1. 创建`FileSniffer`对象，一个`FileSniffer`对象对应监听一个文件。

    ```java
    FileSniffer fs = new FileSniffer(new File("test.log"));
    ```

2. 配置`FileSniffer`对象。

    ```java
    fs.setCacheQueueSize(10000); ①
    fs.setCacheQueueFullPolicy(CacheQueueFullPolicy.DELETE_OLD); ② 
    ```
    > ① 设置一级缓存队列长度
    >
    > ② 设置一级缓存队列满时处理策略
    > 
    > 

3. 配置一级缓存队列满时处理策略丢弃数据监听器

    ```java
    fs.setCacheQueueFullListener( (policy, line) -> {
        // 拒绝策略丢弃的数据在这里输出
        LOGGER.info("丢弃 {} {}", policy, line);
    } );
    ```

4. 添加数据追加监听器

    ```java
    // 创建数据追加监听器实例
    CacheQueueListener pushListener = new CacheQueueListener("group-id-1", "listener-id-1") {
        @Override
        public void process(String newLine) {
            // 处理逻辑写在这里...
        }
    };
    // 添加创建好的监听器
    fs.addCacheQueueListener(pushListener);

    // 可添加多个监听器。多个监听器通过group-id和listener-id进行区分
    // 仿照kafka中Group概念，同一group中的监听器负载分流处理被监听文件的新增数据，不同group复制接收到同样的数据
    fs.addCacheQueueListener(new CacheQueueListener("group-id-2", "listener-id-2") {
        @Override
        public void process(String newLine) {
            // 处理逻辑写在这里...
        }
    });
    ```

5. 删除数据追加监听器

    ```java
    // 可以将已添加的监听器删除
    fs.deleteCacheQueueListener(pushListener);

    // 也可以通过group-id和listener-id进行删除
    fs.deleteCacheQueueListener(CacheQueueListener.of("group-id", "listener-id"));
    ```

6. 启动监听

    ```java
    fs.start();
    ```
    > `start`方法不会阻塞线程，程序退出，则监听退出，如果想阻塞线程，可以使用`startBlockUtilCancel`方法
    ```java
    fs.startBlockUtilCancel();
    ```

7. 关闭监听

    ```java
    fs.close();
    ```