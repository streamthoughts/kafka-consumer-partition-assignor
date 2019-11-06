/*
 * Copyright 2019 StreamThoughts.
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.streamthoughts.kafka.consumer;

import org.apache.kafka.clients.consumer.internals.AbstractPartitionAssignor;
import org.apache.kafka.common.Configurable;
import org.apache.kafka.common.TopicPartition;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public class FailoverAssignor extends AbstractPartitionAssignor implements Configurable {

    private FailoverAssignorConfig config;

    /**
     * {@inheritDoc}
     */
    @Override
    public String name() {
        return "failover";
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void configure(final Map<String, ?> configs) {
        this.config = new FailoverAssignorConfig(configs);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Map<String, List<TopicPartition>> assign(final Map<String, Integer> partitionsPerTopic,
                                                    final Map<String, Subscription> subscriptions) {

        // Generate all topic-partitions using the number of partitions for each subscribed topic.
        final List<TopicPartition> assignments = partitionsPerTopic
            .entrySet()
            .stream()
            .flatMap(entry -> {
                final String topic = entry.getKey();
                final int numPartitions = entry.getValue();
                return IntStream.range(0, numPartitions)
                    .mapToObj( i -> new TopicPartition(topic, i));
            }).collect(Collectors.toList());

        // Decode consumer priority from each subscription and
        Stream<ConsumerPriority> consumerOrdered = subscriptions.entrySet()
            .stream()
            .map(e -> {
                int priority = e.getValue().userData().getInt();
                String memberId = e.getKey();
                return new ConsumerPriority(memberId, priority);
            })
            .sorted(Comparator.reverseOrder());

        // Select the consumer with the highest priority
        ConsumerPriority priority = consumerOrdered.findFirst().get();

        final Map<String, List<TopicPartition>> assign = new HashMap<>();
        subscriptions.keySet().forEach(memberId -> assign.put(memberId, Collections.emptyList()));
        assign.put(priority.memberId, assignments);
        return assign;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Subscription subscription(final Set<String> topics) {
        return new Subscription(new ArrayList<>(topics), encodeUserData());
    }

    private ByteBuffer encodeUserData() {
        return ByteBuffer.allocate(4).putInt(config.priority()).rewind();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void onAssignment(final Assignment assignment) {
        // this assignor maintains no internal state, so nothing to do
    }

    private static class ConsumerPriority implements Comparable<ConsumerPriority> {

        private final String memberId;
        private final Integer priority;

        ConsumerPriority(final String memberId, final Integer priority) {
            this.memberId = memberId;
            this.priority = priority;
        }

        @Override
        public int compareTo(final ConsumerPriority that) {
            return Integer.compare(priority, that.priority);
        }
    }

}
