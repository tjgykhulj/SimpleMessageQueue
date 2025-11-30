package com.arnold.msg.metadata.store;

import com.arnold.msg.JsonUtils;
import com.arnold.msg.ZookeeperClientHolder;
import com.arnold.msg.exceptions.ZookeeperMetadataStoreException;
import com.arnold.msg.metadata.model.Metadata;
import com.arnold.msg.metadata.model.ResourceType;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;

@Slf4j
public class ZookeeperMetadataStore<T extends Metadata> implements MetadataStore<T> {

    private final CuratorFramework client;
    private final ResourceType type;
    private final Class<T> clazz;

    public ZookeeperMetadataStore(ResourceType type, Class<T> clazz) {
        this.client = ZookeeperClientHolder.getClient();
        this.type = type;
        this.clazz = clazz;
    }

    @Override
    public T save(T metadata) {
        String path = getPath(type, metadata.getId());
        byte[] data = JsonUtils.toJsonBytes(metadata);
        try {
            if (client.checkExists().forPath(path) == null) {
                client.create()
                        .creatingParentsIfNeeded()
                        .withMode(CreateMode.PERSISTENT)
                        .forPath(path, data);
            } else {
                client.setData().forPath(path, data);
            }
        } catch (Exception e) {
            throw new ZookeeperMetadataStoreException(e);
        }
        return metadata;
    }

    @Override
    public T deleteByID(String id) {
        String path = getPath(type, id);
        try {
            byte[] data = deleteOnce(path);
            if (data == null) {
                return null;
            }
            return JsonUtils.fromJsonBytes(data, clazz);
        } catch (KeeperException.BadVersionException e) {
            log.warn("delete conflict: {}", path);
            // TODO add max retry attempt
            return deleteByID(id);
        } catch (Exception e) {
            throw new ZookeeperMetadataStoreException(e);
        }
    }

    @Override
    public T findByID(String id) {
        try {
            String path = getPath(type, id);
            byte[] data = client.getData().forPath(path);
            return JsonUtils.fromJsonBytes(data, clazz);
        } catch (Exception e) {
            // return null if not exist
            return null;
        }
    }

    private byte[] deleteOnce(String path) throws Exception {
        Stat stat = client.checkExists().forPath(path);
        if (stat == null) {
            return null;
        }
        byte[] data = client.getData().forPath(path);
        client.delete().withVersion(stat.getVersion()).forPath(path);
        return data;

    }

    private String getPath(ResourceType type, String id) {
        // TODO it should be configurable
        return String.format("/metadata/%s/%s", type, id);
    }
}
