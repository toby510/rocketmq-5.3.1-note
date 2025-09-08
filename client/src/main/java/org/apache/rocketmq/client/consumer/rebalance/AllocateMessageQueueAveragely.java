/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.rocketmq.client.consumer.rebalance;

import java.util.ArrayList;
import java.util.List;
import org.apache.rocketmq.common.message.MessageQueue;

/**
 * Average Hashing queue algorithm
 */
public class AllocateMessageQueueAveragely extends AbstractAllocateMessageQueueStrategy {

    @Override
    public List<MessageQueue> allocate(String consumerGroup, String currentCID, List<MessageQueue> mqAll,
        List<String> cidAll) {

        List<MessageQueue> result = new ArrayList<>();
        if (!check(consumerGroup, currentCID, mqAll, cidAll)) {
            return result;
        }

        //todo @Toby 【队列分配策略-平均分配-1】当前消费者在数组中的位置
        int index = cidAll.indexOf(currentCID);

        //todo @Toby 【队列分配策略-平均分配-2】结合下面的mqAll.size() / cidAll.size()+ 1操作分析得知，这里mod取余结果表示：多分配一个队列的消费者数据
        int mod = mqAll.size() % cidAll.size();

        //todo @Toby 【队列分配策略-平均分配-3】围绕"有mod个消费者，多分配一个队列"的原则进行分配
        //1-如果队列数量小于消费者数量，则每个消费者一个队列，存在分配不到的情况。比如：mqAll={1,2},cidAll={c1,c3,c3,c4},则c3和c4分配不到
        //2-否则，围绕"有mod个消费者，多分配一个队列"的原则进行分配。比如：mqAll={1,2,3,4,5},cidAll={c1,c2,c3},则c1分配{1,2},c3分配{3,4},c3分配{5},即mod=5%d=2,表示前面多消费一个队列的消费者有2个，即让c1和c2多分配一个队列
        int averageSize =
            mqAll.size() <= cidAll.size() ? 1 : (mod > 0 && index < mod ? mqAll.size() / cidAll.size()
                + 1 : mqAll.size() / cidAll.size());
        int startIndex = (mod > 0 && index < mod) ? index * averageSize : index * averageSize + mod;
        int range = Math.min(averageSize, mqAll.size() - startIndex);

        //todo @Toby 【队列分配策略-平均分配-4】按照已确定的rang从mqAll取出队列
        for (int i = 0; i < range; i++) {
            result.add(mqAll.get(startIndex + i));
        }
        return result;
    }

    @Override
    public String getName() {
        return "AVG";
    }
}
