package com.arnold.msg.metadata;

import com.arnold.msg.exceptions.KafkaAdminOperationException;
import com.arnold.msg.metadata.model.ClusterMetadata;
import com.arnold.msg.metadata.model.KafkaClusterMetadata;
import com.arnold.msg.metadata.model.QueueMetadata;
import com.arnold.msg.metadata.opeartor.BackendOperator;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.*;

import java.util.Collections;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

@Slf4j
public class KafkaBackendOperator implements BackendOperator {
    private static final Integer DEFAULT_PARTITION = 3;
    private static final Short DEFAULT_REPLICA = 1;

    @Override
    public void createQueue(ClusterMetadata cluster, QueueMetadata queue) {
        operateInKafka(cluster, adminClient -> {
            try {
                NewTopic topic = new NewTopic(
                        queue.getId(), Optional.of(DEFAULT_PARTITION), Optional.of(DEFAULT_REPLICA));
                CreateTopicsResult result = adminClient.createTopics(Collections.singleton(topic));
                result.all().get();
                log.debug("create topic finish");
            } catch (Exception e) {
                throw new KafkaAdminOperationException("Failed to delete kafka topic", e);
            }
        });
    }

    @Override
    public void deleteQueue(ClusterMetadata metadata, QueueMetadata queue) {
        operateInKafka(metadata, adminClient -> {
            try {
                DeleteTopicsResult result = adminClient.deleteTopics(Collections.singleton(queue.getId()));
                result.all().get();
            } catch (Exception e) {
                throw new KafkaAdminOperationException("Failed to delete kafka topic", e);
            }
        });
    }

    @Override
    public void validateCluster(ClusterMetadata metadata) {
        operateInKafka(metadata, adminClient -> {
            try {
                DescribeClusterResult clusterResult = adminClient.describeCluster();
                String clusterId = clusterResult.clusterId().get(10, TimeUnit.SECONDS);
                log.debug("Kafka connection test passed, cluster id is {}", clusterId);
            } catch (Exception e) {
                throw new KafkaAdminOperationException("Kafka connection validate failed", e);
            }
        });
    }

    private void operateInKafka(ClusterMetadata metadata, Consumer<AdminClient> consumer) {
        KafkaClusterMetadata kafkaMetadata = new KafkaClusterMetadata(metadata);
        Properties props = kafkaMetadata.getConfig().toProperties();
        try (AdminClient adminClient = AdminClient.create(props)) {
            consumer.accept(adminClient);
        }
    }
}
