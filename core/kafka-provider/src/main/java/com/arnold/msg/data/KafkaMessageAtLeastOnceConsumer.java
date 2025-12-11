package com.arnold.msg.data;

import com.arnold.msg.Utils;
import com.arnold.msg.data.model.Message;
import com.arnold.msg.data.model.MessageBatch;
import com.arnold.msg.data.model.MessageBatchAck;
import com.arnold.msg.metadata.model.ConsumerMetadata;
import com.arnold.msg.metadata.model.KafkaClusterConfig;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.LockSupport;

@Slf4j
public class KafkaMessageAtLeastOnceConsumer implements MessageConsumer {

    private final Consumer<byte[], byte[]> client;
    private final String id;
    private final String topic;
    private final int timeoutMs;

    private final ScheduledExecutorService executors;
    private final BatchStateCommitTask batchStateCommitTask;
    private final OffsetCommitTask offsetCommitTask;

    private final Map<Integer, OffsetTracker> offsetTrackerMap;


    public KafkaMessageAtLeastOnceConsumer(KafkaClusterConfig clusterConfig, ConsumerMetadata consumer) {
        // TODO make this configurable in ConsumerMetadata
        this.timeoutMs = 5000;
        Properties props = clusterConfig.toProperties();
        // TODO configured in metadata
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, consumer.getId());
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        this.client = new KafkaConsumer<>(props);
        this.topic = consumer.getQueue();
        this.id = consumer.getId();
        log.debug("Consumer {} created", this.id);
        this.client.subscribe(Collections.singleton(this.topic), new MessageConsumerRebalanceListener());
        this.offsetTrackerMap = new ConcurrentHashMap<>();
        // async task to update batch state and offset trackerMap
        this.batchStateCommitTask = new BatchStateCommitTask(offsetTrackerMap);
        this.offsetCommitTask = new OffsetCommitTask(offsetTrackerMap);
        this.executors = Executors.newScheduledThreadPool(3);
        this.executors.submit(batchStateCommitTask);
        this.executors.scheduleAtFixedRate(offsetCommitTask, 0, 20, TimeUnit.SECONDS);
    }

    @Override
    public MessageBatch poll() {
        MessageBatch batch = new MessageBatch();
        batch.setMessages(new ArrayList<>());
        batch.setBatchId(UUID.randomUUID().toString());

        log.debug("Consumer {} polling", id);
        ConsumerRecords<byte[], byte[]> records = client.poll(Duration.ofMillis(timeoutMs));
        log.debug("Consumer {} polled {} messages", id, records.count());

        // update result
        for (ConsumerRecord<byte[], byte[]> record : records.records(topic)) {
            Message msg = new Message();
            msg.setQueue(topic);
            msg.setPayload(record.value());
            msg.setMessageId(Utils.generateMessageID(record.partition(), record.offset()));
            batch.getMessages().add(msg);
        }
        batchStateCommitTask.addBatchToCommit(batch);
        return batch;
    }

    @Override
    public void acknowledge(MessageBatchAck batchAck) {
        // do nothing in at most once semantic
        log.debug("ack consumer {} with {}", this.id, batchAck);
    }

    class MessageConsumerRebalanceListener implements ConsumerRebalanceListener {

        @Override
        public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
            log.debug("Consumer {} onPartitionsRevoked: {}", id, partitions);
            for (TopicPartition tp : partitions) {
                offsetTrackerMap.remove(tp.partition());
            }
        }

        @Override
        public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
            log.debug("Consumer {} onPartitionsAssigned: {}", id, partitions);
            for (TopicPartition tp : partitions) {
                long currentOff = client.position(tp);
                log.debug("Consumer {} partition {}, offset is {}", id, tp, currentOff);
                offsetTrackerMap.put(tp.partition(), new OffsetTracker(currentOff));
            }
            System.currentTimeMillis();
        }
    }

    class BatchStateCommitTask implements Runnable {

        private final Queue<SimpleMessageBatch> pendingCommitBatch;
        private final Map<Integer, OffsetTracker> offsetTrackerMap;

        BatchStateCommitTask(Map<Integer, OffsetTracker> offsetTrackerMap) {
            this.pendingCommitBatch = new ConcurrentLinkedQueue<>();
            this.offsetTrackerMap = offsetTrackerMap;
        }

        void addBatchToCommit(MessageBatch batch) {
            this.pendingCommitBatch.add(
                    new SimpleMessageBatch(batch));
        }

        @Override
        public void run() {
            while (true) {
                try {
                    runOnce();
                } catch (Exception e) {
                    // TODO retry strategy here
                    log.warn("Failed to poll and commit batch state", e);
                }
            }
        }

        private void runOnce() {
            SimpleMessageBatch batch = pendingCommitBatch.poll();
            if (batch == null) {
                // TODO configure it
                LockSupport.parkNanos(TimeUnit.SECONDS.toNanos(10));
                return;
            }
            // TODO produce this batch
            log.debug("BatchStateCommitTask committed {}", batch);

            // add completed offsets into OffsetTracker
            Map<Integer, MutablePair<Long, Long>> offsetRanges = new HashMap<>();
            for (Long messageId : batch.getMessageIds()) {
                long offset = Utils.getOffset(messageId);
                int partition = Utils.getPartition(messageId);
                if (!offsetTrackerMap.containsKey(partition)) {
                    // already rebalance, no need to track this message
                    continue;
                }
                MutablePair<Long, Long> range = offsetRanges.get(partition);
                if (range == null) {
                    offsetRanges.put(partition, new MutablePair<>(offset, offset));
                } else {
                    // seems no need here, kafka message offsets will always be continuously
                    // range.setLeft(Math.min(offset, range.getLeft()));
                    range.setRight(Math.max(offset, range.getRight()));
                }
            }
            for (Integer partition : offsetRanges.keySet()) {
                MutablePair<Long, Long> range = offsetRanges.get(partition);
                log.debug("complete commit batch state: partition {} from {} to {}",
                        partition, range.getLeft(), range.getRight());
                if (offsetTrackerMap.containsKey(partition)) {
                    offsetTrackerMap.get(partition)
                            .addCompletedOffset(range.getLeft(), range.getRight());
                }
            }
        }
    }

    class OffsetCommitTask implements Runnable {

        private final Map<Integer, OffsetTracker> offsetTrackerMap;

        OffsetCommitTask(Map<Integer, OffsetTracker> offsetTrackerMap) {
            this.offsetTrackerMap = offsetTrackerMap;
        }

        @Override
        public void run() {
            try {
                Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
                offsetTrackerMap.forEach((par, tracker) -> {
                    if (tracker.advanceWatermark()) {
                        offsets.put(new TopicPartition(topic, par), new OffsetAndMetadata(tracker.currentWatermark()));
                    }
                });
                log.info("OffsetCommitTask this round for {}, committed offset {}", id, offsets);
                if (offsets.isEmpty()) {
                    return;
                }
                client.commitAsync(offsets, (ignore, e) -> {
                    if (e != null) {
                        log.error("Failed to commit kafka offset for {} - {}", id, topic, e);
                    }
                });
            } catch (Exception e) {
                log.warn("Failed to do OffsetCommitTask this round", e);
            }
        }
    }
}
