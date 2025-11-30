package com.arnold.msg;

import lombok.Getter;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;

public final class ZookeeperClientHolder {
    @Getter
    private static volatile boolean initialized = false;
    // TODO make it configurable
    private static final String ZK_ADDRESS = "127.0.0.1:2181";
    private static final Object lock = new Object();
    private static volatile CuratorFramework instance;

    private ZookeeperClientHolder() {}

    public static void initialize() {
        if (initialized) {
            return;
        }

        synchronized (lock) {
            if (initialized) {
                return;
            }

            instance = createClient(ZK_ADDRESS);
            instance.start();

            Runtime.getRuntime().addShutdownHook(new Thread(ZookeeperClientHolder::close));
            initialized = true;
        }
    }

    public static CuratorFramework getClient() {
        if (!initialized) {
            throw new IllegalStateException("ZK client not initialized. Call initialize() first.");
        }
        return instance;
    }

    public static void close() {
        synchronized (lock) {
            if (instance != null) {
                try {
                    instance.close();
                    instance = null;
                    initialized = false;
                } catch (Exception e) {
                    System.err.println("Error closing ZK client: " + e.getMessage());
                }
            }
        }
    }

    public static boolean isConnected() {
        return initialized && instance.getZookeeperClient().isConnected();
    }

    private static CuratorFramework createClient(String zkAddress) {
        return CuratorFrameworkFactory.builder()
                .connectString(zkAddress)
                .sessionTimeoutMs(5000)
                .connectionTimeoutMs(5000)
                .retryPolicy(new ExponentialBackoffRetry(1000, 3))
                .build();
    }
}