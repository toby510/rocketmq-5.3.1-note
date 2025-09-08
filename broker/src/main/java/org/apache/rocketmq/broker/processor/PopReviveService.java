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
package org.apache.rocketmq.broker.processor;

import com.alibaba.fastjson.JSON;
import io.opentelemetry.api.common.Attributes;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.metrics.BrokerMetricsManager;
import org.apache.rocketmq.broker.metrics.PopMetricsManager;
import org.apache.rocketmq.client.consumer.PullResult;
import org.apache.rocketmq.client.consumer.PullStatus;
import org.apache.rocketmq.common.KeyBuilder;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.Pair;
import org.apache.rocketmq.common.PopAckConstants;
import org.apache.rocketmq.common.ServiceThread;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.TopicFilterType;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.message.MessageAccessor;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageExtBrokerInner;
import org.apache.rocketmq.common.topic.TopicValidator;
import org.apache.rocketmq.common.utils.DataConverter;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.store.AppendMessageStatus;
import org.apache.rocketmq.store.GetMessageResult;
import org.apache.rocketmq.store.PutMessageResult;
import org.apache.rocketmq.store.pop.AckMsg;
import org.apache.rocketmq.store.pop.BatchAckMsg;
import org.apache.rocketmq.store.pop.PopCheckPoint;

import static org.apache.rocketmq.broker.metrics.BrokerMetricsConstant.LABEL_CONSUMER_GROUP;
import static org.apache.rocketmq.broker.metrics.BrokerMetricsConstant.LABEL_IS_SYSTEM;
import static org.apache.rocketmq.broker.metrics.BrokerMetricsConstant.LABEL_TOPIC;

public class PopReviveService extends ServiceThread {
    private static final Logger POP_LOGGER = LoggerFactory.getLogger(LoggerName.ROCKETMQ_POP_LOGGER_NAME);
    private final int[] ckRewriteIntervalsInSeconds = new int[] { 10, 20, 30, 60, 120, 180, 240, 300, 360, 420, 480, 540, 600, 1200, 1800, 3600, 7200 };

    private int queueId;
    private BrokerController brokerController;
    private String reviveTopic;
    private long currentReviveMessageTimestamp = -1;
    private volatile boolean shouldRunPopRevive = false;

    private final NavigableMap<PopCheckPoint/* oldCK */, Pair<Long/* timestamp */, Boolean/* result */>> inflightReviveRequestMap = Collections.synchronizedNavigableMap(new TreeMap<>());
    private long reviveOffset;

    public PopReviveService(BrokerController brokerController, String reviveTopic, int queueId) {
        this.queueId = queueId;
        this.brokerController = brokerController;
        this.reviveTopic = reviveTopic;
        this.reviveOffset = brokerController.getConsumerOffsetManager().queryOffset(PopAckConstants.REVIVE_GROUP, reviveTopic, queueId);
    }

    @Override
    public String getServiceName() {
        if (brokerController != null && brokerController.getBrokerConfig().isInBrokerContainer()) {
            return brokerController.getBrokerIdentity().getIdentifier() + "PopReviveService_" + this.queueId;
        }
        return "PopReviveService_" + this.queueId;
    }

    public int getQueueId() {
        return queueId;
    }

    public void setShouldRunPopRevive(final boolean shouldRunPopRevive) {
        this.shouldRunPopRevive = shouldRunPopRevive;
    }

    public boolean isShouldRunPopRevive() {
        return shouldRunPopRevive;
    }

    private boolean reviveRetry(PopCheckPoint popCheckPoint, MessageExt messageExt) {
        MessageExtBrokerInner msgInner = new MessageExtBrokerInner();

        //todo @Toby 【pop消费-复活核心逻辑-reviveMsgFromCk复用执行器-4.1】reviveRetry复活终点：
        // 1）若原始消息是 “正常 Topic”（非重试队列）：调用 KeyBuilder.buildPopRetryTopic 构建 Pop 专用重试队列（格式：%RETRY%+消费组+业务Topic）；
        // 2）若原始消息已在重试队列：沿用原重试 Topic，避免重复路由。
        if (!popCheckPoint.getTopic().startsWith(MixAll.RETRY_GROUP_TOPIC_PREFIX)) {
            msgInner.setTopic(KeyBuilder.buildPopRetryTopic(popCheckPoint.getTopic(), popCheckPoint.getCId(), brokerController.getBrokerConfig().isEnableRetryTopicV2()));
        } else {
            msgInner.setTopic(popCheckPoint.getTopic());
        }
        msgInner.setBody(messageExt.getBody());
        msgInner.setQueueId(0);
        if (messageExt.getTags() != null) {
            msgInner.setTags(messageExt.getTags());
        } else {
            MessageAccessor.setProperties(msgInner, new HashMap<>());
        }
        msgInner.setBornTimestamp(messageExt.getBornTimestamp());
        msgInner.setFlag(messageExt.getFlag());
        msgInner.setSysFlag(messageExt.getSysFlag());
        msgInner.setBornHost(brokerController.getStoreHost());
        msgInner.setStoreHost(brokerController.getStoreHost());
        //todo @Toby 重试次数+1,为后续 “死信队列触发”（超过最大重试次数）提供依据；
        msgInner.setReconsumeTimes(messageExt.getReconsumeTimes() + 1);
        msgInner.getProperties().putAll(messageExt.getProperties());
        //todo @Toby 补充 Pop 专用属性：FIRST_POP_TIME（首次拉取时间），便于追溯消息生命周期
        if (messageExt.getReconsumeTimes() == 0 || msgInner.getProperties().get(MessageConst.PROPERTY_FIRST_POP_TIME) == null) {
            msgInner.getProperties().put(MessageConst.PROPERTY_FIRST_POP_TIME, String.valueOf(popCheckPoint.getPopTime()));
        }
        msgInner.setPropertiesString(MessageDecoder.messageProperties2String(msgInner.getProperties()));
        //todo @Toby 若重试队列不存在，自动创建（默认 1 个队列），避免投递时因队列不存在失败，减少人工干预。
        addRetryTopicIfNotExist(msgInner.getTopic(), popCheckPoint.getCId());

        //todo @Toby 【pop消费-复活核心逻辑-reviveMsgFromCk复用执行器-4.2】reviveRetry复活终点：将重试消息投递到指定队列；消费者消费时订阅此队列
        PutMessageResult putMessageResult = brokerController.getEscapeBridge().putMessageToSpecificQueue(msgInner);
        PopMetricsManager.incPopReviveRetryMessageCount(popCheckPoint, putMessageResult.getPutMessageStatus());
        if (brokerController.getBrokerConfig().isEnablePopLog()) {
            POP_LOGGER.info("reviveQueueId={},retry msg, ck={}, msg queueId {}, offset {}, reviveDelay={}, result is {} ",
                queueId, popCheckPoint, messageExt.getQueueId(), messageExt.getQueueOffset(),
                (System.currentTimeMillis() - popCheckPoint.getReviveTime()) / 1000, putMessageResult);
        }
        if (putMessageResult.getAppendMessageResult() == null ||
            putMessageResult.getAppendMessageResult().getStatus() != AppendMessageStatus.PUT_OK) {
            //todo @Toby 若投递失败（AppendMessageStatus != PUT_OK）：返回 false，触发上游rePutCK重试
            POP_LOGGER.error("reviveQueueId={}, revive error, msg is: {}", queueId, msgInner);
            return false;
        }
        this.brokerController.getPopInflightMessageCounter().decrementInFlightMessageNum(popCheckPoint);
        this.brokerController.getBrokerStatsManager().incBrokerPutNums(popCheckPoint.getTopic(), 1);
        this.brokerController.getBrokerStatsManager().incTopicPutNums(msgInner.getTopic());
        this.brokerController.getBrokerStatsManager().incTopicPutSize(msgInner.getTopic(), putMessageResult.getAppendMessageResult().getWroteBytes());
        return true;
    }

    private void initPopRetryOffset(String topic, String consumerGroup) {
        long offset = this.brokerController.getConsumerOffsetManager().queryOffset(consumerGroup, topic, 0);
        if (offset < 0) {
            this.brokerController.getConsumerOffsetManager().commitOffset("initPopRetryOffset", consumerGroup, topic,
                0, 0);
        }
    }

    public void addRetryTopicIfNotExist(String topic, String consumerGroup) {
        if (brokerController != null) {
            TopicConfig topicConfig = brokerController.getTopicConfigManager().selectTopicConfig(topic);
            if (topicConfig != null) {
                return;
            }
            topicConfig = new TopicConfig(topic);
            topicConfig.setReadQueueNums(PopAckConstants.retryQueueNum);
            topicConfig.setWriteQueueNums(PopAckConstants.retryQueueNum);
            topicConfig.setTopicFilterType(TopicFilterType.SINGLE_TAG);
            topicConfig.setPerm(6);
            topicConfig.setTopicSysFlag(0);
            brokerController.getTopicConfigManager().updateTopicConfig(topicConfig);

            initPopRetryOffset(topic, consumerGroup);
        }
    }

    protected List<MessageExt> getReviveMessage(long offset, int queueId) {
        PullResult pullResult = getMessage(PopAckConstants.REVIVE_GROUP, reviveTopic, queueId, offset, 32, true);
        if (pullResult == null) {
            return null;
        }
        if (reachTail(pullResult, offset)) {
            if (this.brokerController.getBrokerConfig().isEnablePopLog()) {
                POP_LOGGER.info("reviveQueueId={}, reach tail,offset {}", queueId, offset);
            }
        } else if (pullResult.getPullStatus() == PullStatus.OFFSET_ILLEGAL || pullResult.getPullStatus() == PullStatus.NO_MATCHED_MSG) {
            POP_LOGGER.error("reviveQueueId={}, OFFSET_ILLEGAL {}, result is {}", queueId, offset, pullResult);
            if (!shouldRunPopRevive) {
                POP_LOGGER.info("slave skip offset correct topic={}, reviveQueueId={}", reviveTopic, queueId);
                return null;
            }
            this.brokerController.getConsumerOffsetManager().commitOffset(PopAckConstants.LOCAL_HOST, PopAckConstants.REVIVE_GROUP, reviveTopic, queueId, pullResult.getNextBeginOffset() - 1);
        }
        return pullResult.getMsgFoundList();
    }

    private boolean reachTail(PullResult pullResult, long offset) {
        return pullResult.getPullStatus() == PullStatus.NO_NEW_MSG
            || pullResult.getPullStatus() == PullStatus.OFFSET_ILLEGAL && offset == pullResult.getMaxOffset();
    }

    // Triple<MessageExt, info, needRetry>
    public CompletableFuture<Triple<MessageExt, String, Boolean>> getBizMessage(PopCheckPoint popCheckPoint, long offset) {
        return this.brokerController.getEscapeBridge().getMessageAsync(popCheckPoint.getTopic(), offset, popCheckPoint.getQueueId(), popCheckPoint.getBrokerName(), false);
    }

    public PullResult getMessage(String group, String topic, int queueId, long offset, int nums,
        boolean deCompressBody) {
        GetMessageResult getMessageResult = this.brokerController.getMessageStore().getMessage(group, topic, queueId, offset, nums, null);

        if (getMessageResult != null) {
            PullStatus pullStatus = PullStatus.NO_NEW_MSG;
            List<MessageExt> foundList = null;
            switch (getMessageResult.getStatus()) {
                case FOUND:
                    pullStatus = PullStatus.FOUND;
                    foundList = decodeMsgList(getMessageResult, deCompressBody);
                    brokerController.getBrokerStatsManager().incGroupGetNums(group, topic, getMessageResult.getMessageCount());
                    brokerController.getBrokerStatsManager().incGroupGetSize(group, topic, getMessageResult.getBufferTotalSize());
                    brokerController.getBrokerStatsManager().incBrokerGetNums(topic, getMessageResult.getMessageCount());
                    brokerController.getBrokerStatsManager().recordDiskFallBehindTime(group, topic, queueId,
                        brokerController.getMessageStore().now() - foundList.get(foundList.size() - 1).getStoreTimestamp());

                    Attributes attributes = BrokerMetricsManager.newAttributesBuilder()
                        .put(LABEL_TOPIC, topic)
                        .put(LABEL_CONSUMER_GROUP, group)
                        .put(LABEL_IS_SYSTEM, TopicValidator.isSystemTopic(topic) || MixAll.isSysConsumerGroup(group))
                        .build();
                    BrokerMetricsManager.messagesOutTotal.add(getMessageResult.getMessageCount(), attributes);
                    BrokerMetricsManager.throughputOutTotal.add(getMessageResult.getBufferTotalSize(), attributes);

                    break;
                case NO_MATCHED_MESSAGE:
                    pullStatus = PullStatus.NO_MATCHED_MSG;
                    POP_LOGGER.debug("no matched message. GetMessageStatus={}, topic={}, groupId={}, requestOffset={}",
                        getMessageResult.getStatus(), topic, group, offset);
                    break;
                case NO_MESSAGE_IN_QUEUE:
                    POP_LOGGER.debug("no new message. GetMessageStatus={}, topic={}, groupId={}, requestOffset={}",
                        getMessageResult.getStatus(), topic, group, offset);
                    break;
                case MESSAGE_WAS_REMOVING:
                case NO_MATCHED_LOGIC_QUEUE:
                case OFFSET_FOUND_NULL:
                case OFFSET_OVERFLOW_BADLY:
                case OFFSET_TOO_SMALL:
                    pullStatus = PullStatus.OFFSET_ILLEGAL;
                    POP_LOGGER.warn("offset illegal. GetMessageStatus={}, topic={}, groupId={}, requestOffset={}",
                        getMessageResult.getStatus(), topic, group, offset);
                    break;
                case OFFSET_OVERFLOW_ONE:
                    // no need to print WARN, because we use "offset + 1" to get the next message
                    pullStatus = PullStatus.OFFSET_ILLEGAL;
                    break;
                default:
                    assert false;
                    break;
            }

            return new PullResult(pullStatus, getMessageResult.getNextBeginOffset(), getMessageResult.getMinOffset(),
                getMessageResult.getMaxOffset(), foundList);

        } else {
            long maxQueueOffset = brokerController.getMessageStore().getMaxOffsetInQueue(topic, queueId);
            if (maxQueueOffset > offset) {
                POP_LOGGER.error("get message from store return null. topic={}, groupId={}, requestOffset={}, maxQueueOffset={}",
                    topic, group, offset, maxQueueOffset);
            }
            return null;
        }
    }

    private List<MessageExt> decodeMsgList(GetMessageResult getMessageResult, boolean deCompressBody) {
        List<MessageExt> foundList = new ArrayList<>();
        try {
            List<ByteBuffer> messageBufferList = getMessageResult.getMessageBufferList();
            if (messageBufferList != null) {
                for (int i = 0; i < messageBufferList.size(); i++) {
                    ByteBuffer bb = messageBufferList.get(i);
                    if (bb == null) {
                        POP_LOGGER.error("bb is null {}", getMessageResult);
                        continue;
                    }
                    MessageExt msgExt = MessageDecoder.decode(bb, true, deCompressBody);
                    if (msgExt == null) {
                        POP_LOGGER.error("decode msgExt is null {}", getMessageResult);
                        continue;
                    }
                    // use CQ offset, not offset in Message
                    msgExt.setQueueOffset(getMessageResult.getMessageQueueOffset().get(i));
                    foundList.add(msgExt);
                }
            }
        } finally {
            getMessageResult.release();
        }

        return foundList;
    }

    //todo @Toby 【pop消费-复活核心逻辑】从下面流程可看出，revive的topic里存在CK和ACK(单个或批量)两种类似的数据
    protected void consumeReviveMessage(ConsumeReviveObj consumeReviveObj) {
        HashMap<String, PopCheckPoint> map = consumeReviveObj.map;
        HashMap<String, PopCheckPoint> mockPointMap = new HashMap<>();
        long startScanTime = System.currentTimeMillis();
        long endTime = 0;

        //todo @Toby 【pop消费-复活核心逻辑-1】拉取复活日志：查询上次复活的偏移量，避免重复拉取日志；取本地记录与远程查询的最大值，确保偏移量不回退。
        long consumeOffset = this.brokerController.getConsumerOffsetManager().queryOffset(PopAckConstants.REVIVE_GROUP, reviveTopic, queueId);
        long oldOffset = Math.max(reviveOffset, consumeOffset);
        consumeReviveObj.oldOffset = oldOffset;
        POP_LOGGER.info("reviveQueueId={}, old offset is {} ", queueId, oldOffset);
        long offset = oldOffset + 1;
        int noMsgCount = 0;
        long firstRt = 0;
        // offset self amend
        while (true) {
            if (!shouldRunPopRevive) {
                POP_LOGGER.info("slave skip scan, revive topic={}, reviveQueueId={}", reviveTopic, queueId);
                break;
            }

            //todo @Toby 【pop消费-复活核心逻辑-1】拉取复活日志：按偏移量从 REVIVE_LOG Topic 拉取消息；
            List<MessageExt> messageExts = getReviveMessage(offset, queueId);
            if (messageExts == null || messageExts.isEmpty()) {
                long old = endTime;
                long timerDelay = brokerController.getMessageStore().getTimerMessageStore().getDequeueBehind();
                long commitLogDelay = brokerController.getMessageStore().getTimerMessageStore().getEnqueueBehind();
                // move endTime
                if (endTime != 0 && System.currentTimeMillis() - endTime > 3 * PopAckConstants.SECOND && timerDelay <= 0 && commitLogDelay <= 0) {
                    endTime = System.currentTimeMillis();
                }
                POP_LOGGER.debug("reviveQueueId={}, offset is {}, can not get new msg, old endTime {}, new endTime {}, timerDelay={}, commitLogDelay={} ",
                    queueId, offset, old, endTime, timerDelay, commitLogDelay);
                if (endTime - firstRt > PopAckConstants.ackTimeInterval + PopAckConstants.SECOND) {
                    break;
                }
                noMsgCount++;
                // Fixme: why sleep is useful here?
                try {
                    Thread.sleep(100);
                } catch (Throwable ignore) {
                }
                if (noMsgCount * 100L > 4 * PopAckConstants.SECOND) {
                    break;
                } else {
                    continue;
                }
            } else {
                noMsgCount = 0;
            }
            if (System.currentTimeMillis() - startScanTime > brokerController.getBrokerConfig().getReviveScanTime()) {
                POP_LOGGER.info("reviveQueueId={}, scan timeout ", queueId);
                break;
            }
            //todo @Toby 【pop消费-复活核心逻辑-1】拉取复活日志:下面逻辑分位2类：
            //1)CK_TAG类型：处理 CheckPoint 消息，解析 PopCheckPoint 对象，用于后面判断是否超时
            //2)ACK类型：这里分为单个和批量ACK，主要做ACK位处理

            for (MessageExt messageExt : messageExts) {
                //todo @Toby 【pop消费-复活核心逻辑-1】拉取复活日志：消息标签为CK_TAG，则topic+cid+queueId+startOffset+popTime+brokerName 作为唯一 Key 存入 map；
                if (PopAckConstants.CK_TAG.equals(messageExt.getTags())) {
                    String raw = new String(messageExt.getBody(), DataConverter.CHARSET_UTF8);
                    if (brokerController.getBrokerConfig().isEnablePopLog()) {
                        POP_LOGGER.info("reviveQueueId={},find ck, offset:{}, raw : {}", messageExt.getQueueId(), messageExt.getQueueOffset(), raw);
                    }
                    PopCheckPoint point = JSON.parseObject(raw, PopCheckPoint.class);
                    if (point.getTopic() == null || point.getCId() == null) {
                        continue;
                    }
                    map.put(point.getTopic() + point.getCId() + point.getQueueId() + point.getStartOffset() + point.getPopTime() + point.getBrokerName(), point);
                    PopMetricsManager.incPopReviveCkGetCount(point, queueId);
                    point.setReviveOffset(messageExt.getQueueOffset());
                    if (firstRt == 0) {
                        //todo @Toby 记录首次锁定时间（firstRt），用于后续超时判定。
                        firstRt = point.getReviveTime();
                    }
                } else if (PopAckConstants.ACK_TAG.equals(messageExt.getTags())) {

                    //todo @Toby 【pop消费-复活核心逻辑-1】拉取复活日志：消息标签为ACK_TAG
                    String raw = new String(messageExt.getBody(), DataConverter.CHARSET_UTF8);
                    if (brokerController.getBrokerConfig().isEnablePopLog()) {
                        POP_LOGGER.info("reviveQueueId={}, find ack, offset:{}, raw : {}", messageExt.getQueueId(), messageExt.getQueueOffset(), raw);
                    }
                    AckMsg ackMsg = JSON.parseObject(raw, AckMsg.class);
                    PopMetricsManager.incPopReviveAckGetCount(ackMsg, queueId);
                    String mergeKey = ackMsg.getTopic() + ackMsg.getConsumerGroup() + ackMsg.getQueueId() + ackMsg.getStartOffset() + ackMsg.getPopTime() + ackMsg.getBrokerName();
                    PopCheckPoint point = map.get(mergeKey);
                    if (point == null) {
                        if (!brokerController.getBrokerConfig().isEnableSkipLongAwaitingAck()) {
                            continue;
                        }
                        //todo @Toby 【pop消费-复活核心逻辑-1】拉取复活日志：否则调用 mockCkForAck 生成 “模拟 CheckPoint”，避免 Ack 消息丢失
                        if (mockCkForAck(messageExt, ackMsg, mergeKey, mockPointMap) && firstRt == 0) {
                            firstRt = mockPointMap.get(mergeKey).getReviveTime();
                        }
                    } else {
                        int indexOfAck = point.indexOfAck(ackMsg.getAckOffset());
                        if (indexOfAck > -1) {
                            //todo @Toby 【pop消费-复活核心逻辑-1】通过setBit(bitMap, index, true) 标记该消息已 Ack（位运算高效标记）；
                            point.setBitMap(DataConverter.setBit(point.getBitMap(), indexOfAck, true));
                        } else {
                            POP_LOGGER.error("invalid ack index, {}, {}", ackMsg, point);
                        }
                    }
                } else if (PopAckConstants.BATCH_ACK_TAG.equals(messageExt.getTags())) {
                    //todo @Toby 【pop消费-复活核心逻辑-1】拉取复活日志：消息标签为BATCH_ACK_TAG，批量ack
                    String raw = new String(messageExt.getBody(), DataConverter.CHARSET_UTF8);
                    if (brokerController.getBrokerConfig().isEnablePopLog()) {
                        POP_LOGGER.info("reviveQueueId={}, find batch ack, offset:{}, raw : {}", messageExt.getQueueId(), messageExt.getQueueOffset(), raw);
                    }

                    BatchAckMsg bAckMsg = JSON.parseObject(raw, BatchAckMsg.class);
                    PopMetricsManager.incPopReviveAckGetCount(bAckMsg, queueId);
                    String mergeKey = bAckMsg.getTopic() + bAckMsg.getConsumerGroup() + bAckMsg.getQueueId() + bAckMsg.getStartOffset() + bAckMsg.getPopTime() + bAckMsg.getBrokerName();
                    PopCheckPoint point = map.get(mergeKey);
                    if (point == null) {
                        if (!brokerController.getBrokerConfig().isEnableSkipLongAwaitingAck()) {
                            continue;
                        }
                        //todo @Toby 【pop消费-复活核心逻辑-1】拉取复活日志：否则调用 mockCkForAck 生成 “模拟 CheckPoint”，避免 Ack 消息丢失
                        if (mockCkForAck(messageExt, bAckMsg, mergeKey, mockPointMap) && firstRt == 0) {
                            firstRt = mockPointMap.get(mergeKey).getReviveTime();
                        }
                    } else {
                        List<Long> ackOffsetList = bAckMsg.getAckOffsetList();
                        for (Long ackOffset : ackOffsetList) {
                            int indexOfAck = point.indexOfAck(ackOffset);
                            if (indexOfAck > -1) {
                               //todo @Toby 【pop消费-复活核心逻辑-1】通过setBit(bitMap, index, true) 标记该消息已 Ack（位运算高效标记）；
                                point.setBitMap(DataConverter.setBit(point.getBitMap(), indexOfAck, true));
                            } else {
                                POP_LOGGER.error("invalid batch ack index, {}, {}", bAckMsg, point);
                            }
                        }
                    }
                }
                long deliverTime = messageExt.getDeliverTimeMs();
                if (deliverTime > endTime) {
                    endTime = deliverTime;
                }
            }
            offset = offset + messageExts.size();
        }
        consumeReviveObj.map.putAll(mockPointMap);
        //todo @Toby 【pop消费-复活核心逻辑-1】记录日志的最新投递时间，用于后续超时判定（endTime - reviveTime 判定是否超时）。
        consumeReviveObj.endTime = endTime;
    }

    private boolean mockCkForAck(MessageExt messageExt, AckMsg ackMsg, String mergeKey, HashMap<String, PopCheckPoint> mockPointMap) {
        long ackWaitTime = System.currentTimeMillis() - messageExt.getDeliverTimeMs();
        long reviveAckWaitMs = brokerController.getBrokerConfig().getReviveAckWaitMs();
        if (ackWaitTime > reviveAckWaitMs) {
            // will use the reviveOffset of popCheckPoint to commit offset in mergeAndRevive
            PopCheckPoint mockPoint = createMockCkForAck(ackMsg, messageExt.getQueueOffset());
            POP_LOGGER.warn(
                    "ack wait for {}ms cannot find ck, skip this ack. mergeKey:{}, ack:{}, mockCk:{}",
                    reviveAckWaitMs, mergeKey, ackMsg, mockPoint);
            mockPointMap.put(mergeKey, mockPoint);
            return true;
        }
        return false;
    }

    private PopCheckPoint createMockCkForAck(AckMsg ackMsg, long reviveOffset) {
        PopCheckPoint point = new PopCheckPoint();
        point.setStartOffset(ackMsg.getStartOffset());
        point.setPopTime(ackMsg.getPopTime());
        point.setQueueId(ackMsg.getQueueId());
        point.setCId(ackMsg.getConsumerGroup());
        point.setTopic(ackMsg.getTopic());
        point.setNum((byte) 0);
        point.setBitMap(0);
        point.setReviveOffset(reviveOffset);
        point.setBrokerName(ackMsg.getBrokerName());
        return point;
    }

    /**
     *  //todo @Toby 【pop消费-复活核心逻辑-mergeAndRevive复活筛选-0】作用：
     *  从 “待处理 CheckPoint” 中筛选出 “真正需要复活的超时消息”，同时通过并发控制与资源校验，确保复活流程的 “准确性” 与 “安全性”。
     * @param consumeReviveObj
     * @throws Throwable
     */
    protected void mergeAndRevive(ConsumeReviveObj consumeReviveObj) throws Throwable {

        //todo @Toby 【pop消费-复活核心逻辑-mergeAndRevive复活筛选-1】按 reviveTime（锁定时间）升序排序，确保 “先锁定的消息优先复活”，符合消息消费的顺序性。
        ArrayList<PopCheckPoint> sortList = consumeReviveObj.genSortList();
        POP_LOGGER.info("reviveQueueId={}, ck listSize={}", queueId, sortList.size());
        if (sortList.size() != 0) {
            POP_LOGGER.info("reviveQueueId={}, 1st ck, startOffset={}, reviveOffset={}; last ck, startOffset={}, reviveOffset={}", queueId, sortList.get(0).getStartOffset(),
                sortList.get(0).getReviveOffset(), sortList.get(sortList.size() - 1).getStartOffset(), sortList.get(sortList.size() - 1).getReviveOffset());
        }
        long newOffset = consumeReviveObj.oldOffset;
        for (PopCheckPoint popCheckPoint : sortList) {
            if (!shouldRunPopRevive) {
                POP_LOGGER.info("slave skip ck process, revive topic={}, reviveQueueId={}", reviveTopic, queueId);
                break;
            }
            //todo @Toby 【pop消费-复活核心逻辑-mergeAndRevive复活筛选-2】没有到复活时间，跳过。因列表有序，未超时则后续消息更晚超时，直接 break 循环，提升效率
            if (consumeReviveObj.endTime - popCheckPoint.getReviveTime() <= (PopAckConstants.ackTimeInterval + PopAckConstants.SECOND)) {
                break;
            }

            // check normal topic, skip ck , if normal topic is not exist
            //todo @Toby 【pop消费-复活核心逻辑-mergeAndRevive复活筛选-3】topic校验，不存在则跳过，避免复活无效 Topic 的消息；
            String normalTopic = KeyBuilder.parseNormalTopic(popCheckPoint.getTopic(), popCheckPoint.getCId());
            if (brokerController.getTopicConfigManager().selectTopicConfig(normalTopic) == null) {
                POP_LOGGER.warn("reviveQueueId={}, can not get normal topic {}, then continue", queueId, popCheckPoint.getTopic());
                newOffset = popCheckPoint.getReviveOffset();
                continue;
            }
            //todo @Toby 【pop消费-复活核心逻辑-mergeAndRevive复活筛选-4】订阅校验，避免消费组已删除导致复活消息无人消费。
            if (null == brokerController.getSubscriptionGroupManager().findSubscriptionGroupConfig(popCheckPoint.getCId())) {
                POP_LOGGER.warn("reviveQueueId={}, can not get cid {}, then continue", queueId, popCheckPoint.getCId());
                newOffset = popCheckPoint.getReviveOffset();
                continue;
            }

            //todo @Toby 【pop消费-复活核心逻辑-mergeAndRevive复活筛选-5】并发校验，限制同时处理的复活任务不超过 3 个（避免资源过载）；
            while (inflightReviveRequestMap.size() > 3) {
                waitForRunning(100);
                Pair<Long, Boolean> pair = inflightReviveRequestMap.firstEntry().getValue();
                if (!pair.getObject2() && System.currentTimeMillis() - pair.getObject1() > 1000 * 30) {
                    PopCheckPoint oldCK = inflightReviveRequestMap.firstKey();
                    rePutCK(oldCK, pair);
                    inflightReviveRequestMap.remove(oldCK);
                    POP_LOGGER.warn("stay too long, remove from reviveRequestMap, {}, {}, {}, {}", popCheckPoint.getTopic(),
                            popCheckPoint.getBrokerName(), popCheckPoint.getQueueId(), popCheckPoint.getStartOffset());
                }
            }

            //todo @Toby 【pop消费-复活核心逻辑-mergeAndRevive复活筛选-6】触发复活reviveMsgFromCk，对单个超时 CheckPoint 执行复活
            reviveMsgFromCk(popCheckPoint);

            newOffset = popCheckPoint.getReviveOffset();
        }
        if (newOffset > consumeReviveObj.oldOffset) {
            if (!shouldRunPopRevive) {
                POP_LOGGER.info("slave skip commit, revive topic={}, reviveQueueId={}", reviveTopic, queueId);
                return;
            }
            this.brokerController.getConsumerOffsetManager().commitOffset(PopAckConstants.LOCAL_HOST, PopAckConstants.REVIVE_GROUP, reviveTopic, queueId, newOffset);
        }
        reviveOffset = newOffset;
        consumeReviveObj.newOffset = newOffset;
    }
    //todo @Toby 【pop消费-复活核心逻辑-reviveMsgFromCk复用执行器-0】
    private void reviveMsgFromCk(PopCheckPoint popCheckPoint) {
        if (!shouldRunPopRevive) {
            POP_LOGGER.info("slave skip retry, revive topic={}, reviveQueueId={}", reviveTopic, queueId);
            return;
        }
        //todo @Toby 【pop消费-复活核心逻辑-reviveMsgFromCk复用执行器-1】将当前 CheckPoint 注册为 “正在处理的复活任务”，标记状态为 “未完成”，便于后续跟踪。
        inflightReviveRequestMap.put(popCheckPoint, new Pair<>(System.currentTimeMillis(), false));
        List<CompletableFuture<Pair<Long, Boolean>>> futureList = new ArrayList<>(popCheckPoint.getNum());
        for (int j = 0; j < popCheckPoint.getNum(); j++) {
            //todo @Toby 【pop消费-复活核心逻辑-reviveMsgFromCk复用执行器-2】通过位运算（bitMap）快速判断第 j 条消息是否已 Ack，已 Ack 则跳过，避免无效复活。如果已经ack则跳过
            if (DataConverter.getBit(popCheckPoint.getBitMap(), j)) {
                continue;
            }

            // retry msg
            long msgOffset = popCheckPoint.ackOffsetByIndex((byte) j);
            //todo @Toby 【pop消费-复活核心逻辑-reviveMsgFromCk复用执行器-3】没有ack的，异步从 Broker 存储中查询原始锁定消息（非阻塞，提升并发效率）
            CompletableFuture<Pair<Long, Boolean>> future = getBizMessage(popCheckPoint, msgOffset)
                .thenApply(rst -> {
                    MessageExt message = rst.getLeft();
                    if (message == null) {
                        //todo @Toby 查询失败，返回 “需要重试” 结果（!rst.getRight()），后续通过 rePutCK 重新入队。
                        POP_LOGGER.info("reviveQueueId={}, can not get biz msg, topic:{}, qid:{}, offset:{}, brokerName:{}, info:{}, retry:{}, then continue",
                            queueId, popCheckPoint.getTopic(), popCheckPoint.getQueueId(), msgOffset, popCheckPoint.getBrokerName(), UtilAll.frontStringAtLeast(rst.getMiddle(), 60), rst.getRight());
                        return new Pair<>(msgOffset, !rst.getRight()); // Pair.object2 means OK or not, Triple.right value means needRetry
                    }

                    //todo @Toby 【pop消费-复活核心逻辑-reviveMsgFromCk复用执行器-4】执行单条消息复活：调用 reviveRetry 将原始消息投递到重试队列（复活的最终落地）。
                    boolean result = reviveRetry(popCheckPoint, message);
                    return new Pair<>(msgOffset, result);
                });
            futureList.add(future);
        }
        CompletableFuture.allOf(futureList.toArray(new CompletableFuture[0]))
            .whenComplete((v, e) -> {
                //todo 所有异步任务完成后，遍历结果
                for (CompletableFuture<Pair<Long, Boolean>> future : futureList) {
                    Pair<Long, Boolean> pair = future.getNow(new Pair<>(0L, false));
                    if (!pair.getObject2()) {
                        //todo @Toby 【pop消费-复活核心逻辑-reviveMsgFromCk复用执行器-5】复活失败,重新入队（rePutCK），确保临时失败的消息能重试；
                        rePutCK(popCheckPoint, pair);
                    }
                }

                if (inflightReviveRequestMap.containsKey(popCheckPoint)) {
                    //todo @Toby 【pop消费-复活核心逻辑-reviveMsgFromCk复用执行器-6】复活成功,标记任务状态为 “已完成”
                    inflightReviveRequestMap.get(popCheckPoint).setObject2(true);
                }
                for (Map.Entry<PopCheckPoint, Pair<Long, Boolean>> entry : inflightReviveRequestMap.entrySet()) {
                    PopCheckPoint oldCK = entry.getKey();
                    Pair<Long, Boolean> pair = entry.getValue();
                    if (pair.getObject2()) {
                        //todo @Toby 【pop消费-复活核心逻辑-reviveMsgFromCk复用执行器-7】对已完成的任务，提交复活偏移量，避免重复处理;复活成功,并清理任务缓存
                        brokerController.getConsumerOffsetManager().commitOffset(PopAckConstants.LOCAL_HOST, PopAckConstants.REVIVE_GROUP, reviveTopic, queueId, oldCK.getReviveOffset());
                        inflightReviveRequestMap.remove(oldCK);
                    } else {
                        break;
                    }
                }
            });
    }

    private void rePutCK(PopCheckPoint oldCK, Pair<Long, Boolean> pair) {
        int rePutTimes = oldCK.parseRePutTimes();
        if (rePutTimes >= ckRewriteIntervalsInSeconds.length && brokerController.getBrokerConfig().isSkipWhenCKRePutReachMaxTimes()) {
            POP_LOGGER.warn("rePut CK reach max times, drop it. {}, {}, {}, {}-{}, {}, {}, {}", oldCK.getTopic(), oldCK.getCId(),
                    oldCK.getBrokerName(), oldCK.getQueueId(), pair.getObject1(), oldCK.getPopTime(), oldCK.getInvisibleTime(), rePutTimes);
            return;
        }

        PopCheckPoint newCk = new PopCheckPoint();
        newCk.setBitMap(0);
        newCk.setNum((byte) 1);
        newCk.setPopTime(oldCK.getPopTime());
        newCk.setInvisibleTime(oldCK.getInvisibleTime());
        newCk.setStartOffset(pair.getObject1());
        newCk.setCId(oldCK.getCId());
        newCk.setTopic(oldCK.getTopic());
        newCk.setQueueId(oldCK.getQueueId());
        newCk.setBrokerName(oldCK.getBrokerName());
        newCk.addDiff(0);
        newCk.setRePutTimes(String.valueOf(rePutTimes + 1)); // always increment even if removed from reviveRequestMap
        if (oldCK.getReviveTime() <= System.currentTimeMillis()) {
            // never expect an ACK matched in the future, we just use it to rewrite CK and try to revive retry message next time
            int intervalIndex = rePutTimes >= ckRewriteIntervalsInSeconds.length ? ckRewriteIntervalsInSeconds.length - 1 : rePutTimes;
            newCk.setInvisibleTime(oldCK.getInvisibleTime() + ckRewriteIntervalsInSeconds[intervalIndex] * 1000);
        }
        MessageExtBrokerInner ckMsg = brokerController.getPopMessageProcessor().buildCkMsg(newCk, queueId);
        brokerController.getMessageStore().putMessage(ckMsg);
    }

    public long getReviveBehindMillis() {
        if (currentReviveMessageTimestamp <= 0) {
            return 0;
        }
        long maxOffset = brokerController.getMessageStore().getMaxOffsetInQueue(reviveTopic, queueId);
        if (maxOffset - reviveOffset > 1) {
            return Math.max(0, System.currentTimeMillis() - currentReviveMessageTimestamp);
        }
        return 0;
    }

    public long getReviveBehindMessages() {
        if (currentReviveMessageTimestamp <= 0) {
            return 0;
        }
        long diff = brokerController.getMessageStore().getMaxOffsetInQueue(reviveTopic, queueId) - reviveOffset;
        return Math.max(0, diff);
    }

    @Override
    public void run() {
        int slow = 1;
        while (!this.isStopped()) {
            try {
                if (System.currentTimeMillis() < brokerController.getShouldStartTime()) {
                    POP_LOGGER.info("PopReviveService Ready to run after {}", brokerController.getShouldStartTime());
                    this.waitForRunning(1000);
                    continue;
                }
                this.waitForRunning(brokerController.getBrokerConfig().getReviveInterval());
                //todo @Toby 【pop消费-复活核心逻辑-0】过滤：只有master几点有资格执行
                if (!shouldRunPopRevive) {
                    POP_LOGGER.info("skip start revive topic={}, reviveQueueId={}", reviveTopic, queueId);
                    continue;
                }

                if (!brokerController.getMessageStore().getMessageStoreConfig().isTimerWheelEnable()) {
                    POP_LOGGER.warn("skip revive topic because timerWheelEnable is false");
                    continue;
                }

                POP_LOGGER.info("start revive topic={}, reviveQueueId={}", reviveTopic, queueId);
                ConsumeReviveObj consumeReviveObj = new ConsumeReviveObj();
                //todo @Toby 【pop消费-复活核心逻辑-1】拉取复活日志，是复活流程的 “数据来源“。
                // 核心作用：”完成 “CheckPoint 与 Ack 的匹配”，筛选出 “已 Ack 的消息”（标记跳过）和 “未 Ack 的消息”（待复活），为后续复活提供准确的数据源。
                consumeReviveMessage(consumeReviveObj);

                if (!shouldRunPopRevive) {
                    POP_LOGGER.info("slave skip scan, revive topic={}, reviveQueueId={}", reviveTopic, queueId);
                    continue;
                }

                //todo @Toby 【pop消费-复活核心逻辑-1】
                mergeAndRevive(consumeReviveObj);

                ArrayList<PopCheckPoint> sortList = consumeReviveObj.sortList;
                long delay = 0;
                if (sortList != null && !sortList.isEmpty()) {
                    delay = (System.currentTimeMillis() - sortList.get(0).getReviveTime()) / 1000;
                    currentReviveMessageTimestamp = sortList.get(0).getReviveTime();
                    slow = 1;
                } else {
                    currentReviveMessageTimestamp = System.currentTimeMillis();
                }

                POP_LOGGER.info("reviveQueueId={}, revive finish,old offset is {}, new offset is {}, ckDelay={}  ",
                    queueId, consumeReviveObj.oldOffset, consumeReviveObj.newOffset, delay);

                if (sortList == null || sortList.isEmpty()) {
                    POP_LOGGER.info("reviveQueueId={}, has no new msg, take a rest {}", queueId, slow);
                    this.waitForRunning(slow * brokerController.getBrokerConfig().getReviveInterval());
                    if (slow < brokerController.getBrokerConfig().getReviveMaxSlow()) {
                        slow++;
                    }
                }

            } catch (Throwable e) {
                POP_LOGGER.error("reviveQueueId={}, revive error", queueId, e);
            }
        }
    }

    static class ConsumeReviveObj {
        HashMap<String, PopCheckPoint> map = new HashMap<>();
        ArrayList<PopCheckPoint> sortList;
        long oldOffset;
        long endTime;
        long newOffset;

        ArrayList<PopCheckPoint> genSortList() {
            if (sortList != null) {
                return sortList;
            }
            sortList = new ArrayList<>(map.values());
            sortList.sort((o1, o2) -> (int) (o1.getReviveOffset() - o2.getReviveOffset()));
            return sortList;
        }
    }
}
