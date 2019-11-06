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

import org.apache.kafka.clients.consumer.internals.PartitionAssignor;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

class FailoverAssignorTest {

    private static final Set<String> TOPICS;

    static {
        TOPICS = new HashSet<>();
        TOPICS.add("topic-test");
    }

    @Test
    public void shouldAssignAllAvailablePartitionToConsumerWithHighestPriority() {

        FailoverAssignor consumer1 = new FailoverAssignor();
        consumer1.configure(Collections.singletonMap(FailoverAssignorConfig.CONSUMER_PRIORITY_CONFIG, 1));

        FailoverAssignor consumer2 = new FailoverAssignor();
        consumer2.configure(Collections.singletonMap(FailoverAssignorConfig.CONSUMER_PRIORITY_CONFIG, 2));

        FailoverAssignor consumer3 = new FailoverAssignor();
        consumer3.configure(Collections.singletonMap(FailoverAssignorConfig.CONSUMER_PRIORITY_CONFIG, 3));

        Map<String, PartitionAssignor.Subscription> subscriptions = new HashMap<>();
        subscriptions.put("member1", consumer1.subscription(TOPICS));
        subscriptions.put("member2", consumer2.subscription(TOPICS));
        subscriptions.put("member3", consumer3.subscription(TOPICS));

        Map<String, List<TopicPartition>> assign = consumer1.assign(Collections.singletonMap("topic-test", 3), subscriptions);

        Assertions.assertEquals(1, assign.size());

        Map.Entry<String, List<TopicPartition>> entry = assign.entrySet().iterator().next();
        Assertions.assertEquals("member3", entry.getKey());
        Assertions.assertEquals(3, entry.getValue().size());
    }
}