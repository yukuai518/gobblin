/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.gobblin.instrumented;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.apache.gobblin.metrics.ContextAwareMetric;
import org.apache.gobblin.metrics.MetricContext;
import org.apache.gobblin.metrics.Tag;

import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricSet;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;


/**
 * This interface indicates a class will expose its metrics to some external systems.
 */
public interface StandardMetricsBridge extends Instrumentable {

  StandardMetrics getStandardMetrics();

  default void switchMetricContext(MetricContext context) {
    throw new UnsupportedOperationException();
  }

  default void switchMetricContext(List<Tag<?>> tags) {
    throw new UnsupportedOperationException();
  }

  default List<Tag<?>> generateTags(org.apache.gobblin.configuration.State state) {
    return ImmutableList.of();
  }

  public class StandardMetrics implements MetricSet {

    public String getName() {
      return this.getClass().getName();
    }

    public Collection<ContextAwareMetric> getContextAwareMetrics() {
      return ImmutableList.of();
    }

    public Map<String, Metric> getMetrics() {
      return Maps.newHashMap();
    }
  }
}
