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

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

import java.util.Map;

public class FailoverAssignorConfig extends AbstractConfig {

    public static final String CONSUMER_PRIORITY_CONFIG = "assignment.consumer.priority";
    public static final String CONSUMER_PRIORITY_DOC = "The priority attached to the consumer that must be used for assigning partitions. " +
            "Available partitions for subscribed topics are assigned to the consumer with the highest priority within the group.";

    private static final ConfigDef CONFIG;

    static {
        CONFIG = new ConfigDef()
                .define(CONSUMER_PRIORITY_CONFIG, ConfigDef.Type.INT, Integer.MIN_VALUE,
                        ConfigDef.Importance.HIGH, CONSUMER_PRIORITY_DOC);
    }

    public FailoverAssignorConfig(final Map<?, ?> originals) {
        super(CONFIG, originals);
    }

    public int priority() {
        return getInt(CONSUMER_PRIORITY_CONFIG);
    }
}
