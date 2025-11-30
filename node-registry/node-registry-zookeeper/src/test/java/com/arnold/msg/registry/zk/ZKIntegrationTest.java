package com.arnold.msg.registry.zk;

import com.arnold.msg.ZookeeperBootstrap;
import com.arnold.msg.registry.DataNode;
import org.junit.jupiter.api.Test;

public class ZKIntegrationTest {

    @Test
    public void testCase1() throws Exception {
        test(new DataNode("127.0.0.1", 8080));
    }

    @Test
    public void testCase2() throws Exception {
        test(new DataNode("127.0.0.1", 8081));
    }

    private void test(DataNode node) throws Exception {
        ZookeeperBootstrap.initClient();
        ZookeeperNodeRegistry registry = new ZookeeperNodeRegistry("127.0.0.1:2181", "/test");
        String path = registry.registerNode(node);

        System.out.println(path);
        int i = System.in.read();
    }
}
