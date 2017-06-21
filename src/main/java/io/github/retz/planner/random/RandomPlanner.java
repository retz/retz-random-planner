/**
 *    Retz Random Planner
 *    Copyright (C) 2017 Nautilus Technologies, Inc.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */
package io.github.retz.planner.random;

import io.github.retz.planner.spi.Attribute;
import io.github.retz.planner.spi.Plan;
import io.github.retz.planner.spi.Offer;
import io.github.retz.planner.spi.Planner;
import io.github.retz.protocol.data.Job;
import io.github.retz.protocol.data.ResourceQuantity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class RandomPlanner implements Planner{
    private static final Logger LOG = LoggerFactory.getLogger(RandomPlanner.class);
    private static final List<String> ORDER_BY = Arrays.asList("id");
    private Random random;
    private int maxStock;

    @Override
    public void initialize(Properties p) throws Throwable {
        random = new Random();
    }

    @Override
    public boolean filter(Job job) {
        // No job is removed from candidates
        return true;
    }

    @Override
    public void setMaxStock(int maxStock) {
        LOG.info("setmaxstock = {}", maxStock);
        this.maxStock = maxStock;
    }

    @Override
    public void setUseGpu(boolean useGpu) {
    }

    @Override
    public List<String> orderBy() {
        return ORDER_BY;
    }

    @Override
    public Plan plan(Map<String, Offer> offers, List<Job> jobs) {
        List<Job> queue = new LinkedList<>(jobs);
        Plan plan = new Plan();

        for (Map.Entry<String, Offer> entry : offers.entrySet()) {
            LOG.info("Offer({}), {} attributes, resource={}", entry.getKey(),
                    entry.getValue().attributes().size(), entry.getValue().resource());
            for(Attribute attr: entry.getValue().attributes()) {
                LOG.info("attr: {}", attr);
            }
            if (queue.size() == 0) {
                if (plan.getOfferIdsToStock().size() < maxStock) {
                    plan.addStock(entry.getKey());
                    continue;
                } else {
                    return plan;
                }
            }
            int i = random.nextInt(queue.size());
            if (entry.getValue().resource().toQuantity().fits(queue.get(i))) {
                plan.setJob(entry.getKey(), queue.remove(i));
            } else if (plan.getOfferIdsToStock().size() < maxStock) {
                plan.addStock(entry.getKey());
            }
        }
        LOG.info("Random plan: {}", plan);
        return plan;
    }
}
