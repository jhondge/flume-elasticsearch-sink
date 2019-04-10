/*
 * Copyright 2017 Cognitree Technologies
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */
package com.cognitree.flume.sink.elasticsearch;

import com.cognitree.flume.sink.elasticsearch.client.BulkProcessorBulider;
import com.cognitree.flume.sink.elasticsearch.client.ElasticsearchClientBuilder;
import com.google.common.base.Charsets;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.Transaction;
import org.apache.flume.conf.Configurable;
import org.apache.flume.event.SimpleEvent;
import org.apache.flume.sink.AbstractSink;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.transport.NoNodeAvailableException;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.cognitree.flume.sink.elasticsearch.Constants.*;

/**
 * This sink will read the events from a channel and add them to the bulk processor.
 * <p>
 * This sink must be configured with mandatory parameters detailed in
 * {@link Constants}
 */
public class ElasticSearchSink extends AbstractSink implements Configurable {

    private static final Logger logger = LoggerFactory.getLogger(ElasticSearchSink.class);

    private static final int CHECK_CONNECTION_PERIOD = 3000;

    private BulkProcessor bulkProcessor;

    private IndexBuilder indexBuilder;

    private Serializer serializer;

    private RestHighLevelClient client;

    private AtomicBoolean shouldBackOff = new AtomicBoolean(false);

    public RestHighLevelClient getClient() {
        return client;
    }

    protected Pattern eventPrefixRegex;

    private Event previousEvent;

    @Override
    public void configure(Context context) {
        String[] hosts = getHosts(context);
        if (ArrayUtils.isNotEmpty(hosts)) {
            client = new ElasticsearchClientBuilder(
                    context.getString(PREFIX + ES_CLUSTER_NAME, DEFAULT_ES_CLUSTER_NAME), hosts)
                    .build();
            buildIndexBuilder(context);
            buildSerializer(context);
            bulkProcessor = new BulkProcessorBulider().buildBulkProcessor(context, this);
        } else {
            logger.error("Could not create Rest client, No host exist");
        }
        String eventStartRegex = context.getString(ES_EVENT_PREFIX_REGEX);
        if(!Strings.isNullOrEmpty(eventStartRegex)){
            this.eventPrefixRegex = Pattern.compile(eventStartRegex);
            logger.info("will check the line prefix as the event:{}", eventStartRegex);
        }
    }

    @Override
    public Status process() {
        if (shouldBackOff.get()) {
            throw new NoNodeAvailableException("Check whether Elasticsearch is down or not.");
        }
        Channel channel = getChannel();
        Transaction txn = channel.getTransaction();
        txn.begin();
        try {
            Event event = channel.take();
            if (event != null) {
                String body = new String(event.getBody(), Charsets.UTF_8);
                if (!Strings.isNullOrEmpty(body)) {
                    if(this.eventPrefixRegex != null) {
                        //check the prefix is match
                        JsonParser parser = new JsonParser();
                        JsonElement element = parser.parse(body);
                        JsonObject jsonObject = element.getAsJsonObject();
                        String logStr = jsonObject.get("log").getAsString();

                        Matcher matcher = this.eventPrefixRegex.matcher(logStr);
                        if(matcher.find()) {
                            //flush the pre event, if null don't do anything
                            if(previousEvent != null) {
                              //flush the pre event
                                String preBody = new String(previousEvent.getBody(), Charsets.UTF_8);
                                flushEvent(previousEvent, preBody);
                                //如果内容没有上一条消息，直接写入ES，并且将previousEvent设置为null 以便后续设置为当前值
                                previousEvent = null;
                            } else {
                                logger.debug("new event without previous event in memory, delay write when next event occur");
                            }
                        } else if( previousEvent != null){
                            //append event
                            String preBody = new String(previousEvent.getBody(), Charsets.UTF_8);
                            logger.info(">>>append body to previous, previous:{}", preBody);
                            if(!Strings.isNullOrEmpty(preBody)) {
                                JsonParser preparser = new JsonParser();
                                JsonElement preelement = preparser.parse(preBody);
                                JsonObject prejsonObject = preelement.getAsJsonObject();
                                String prelogStr = prejsonObject.get("log").getAsString();
                                StringBuilder builder = new StringBuilder();
                                builder.append(prelogStr);
                                builder.append(logStr);

                                prejsonObject.addProperty("log", builder.toString());

                                String newBody = prejsonObject.toString();

                                logger.info("<<<append body result:{}", newBody);
                                previousEvent.setBody(newBody.getBytes());
                            } else {
                                logger.error("<<<previous event occur error:{}", previousEvent);
                            }
                        }
                        if( previousEvent == null){
                            previousEvent = new SimpleEvent();
                            previousEvent.setHeaders(event.getHeaders());
                            previousEvent.setBody(event.getBody());
                        }
                    } else {
                        flushEvent(event, body);
                    }
                }
            }
            txn.commit();
            return Status.READY;
        } catch (Throwable tx) {
            try {
                txn.rollback();
            } catch (Exception ex) {
                logger.error("exception in rollback.", ex);
            }
            logger.error("transaction rolled back.", tx);
            return Status.BACKOFF;
        } finally {
            txn.close();
        }
    }

    private void flushEvent(Event event, String body) {
        logger.debug("start to sink event [{}].", body);
        String index = indexBuilder.getIndex(event);
        String type = indexBuilder.getType(event);
        String id = indexBuilder.getId(event);
        XContentBuilder xContentBuilder = serializer.serialize(event);
        if (xContentBuilder != null) {
            if (!(Strings.isNullOrEmpty(id))) {
                bulkProcessor.add(new IndexRequest(index, type, id)
                        .source(xContentBuilder));
            } else {
                bulkProcessor.add(new IndexRequest(index, type)
                        .source(xContentBuilder));
            }
        } else {
            logger.error("Could not serialize the event body [{}] for index [{}], type[{}] and id [{}] ",
                    new Object[]{body, index, type, id});
        }
        logger.debug("sink event [{}] successfully.", body);
    }

    @Override
    public void stop() {
        if (bulkProcessor != null) {
            bulkProcessor.close();
        }
    }

    /**
     * builds Index builder
     */
    private void buildIndexBuilder(Context context) {
        String indexBuilderClass = DEFAULT_ES_INDEX_BUILDER;
        if (StringUtils.isNotBlank(context.getString(ES_INDEX_BUILDER))) {
            indexBuilderClass = context.getString(ES_INDEX_BUILDER);
        }
        this.indexBuilder = instantiateClass(indexBuilderClass);
        if (this.indexBuilder != null) {
            this.indexBuilder.configure(context);
        }
    }

    /**
     * builds Serializer
     */
    private void buildSerializer(Context context) {
        String serializerClass = DEFAULT_ES_SERIALIZER;
        if (StringUtils.isNotEmpty(context.getString(ES_SERIALIZER))) {
            serializerClass = context.getString(ES_SERIALIZER);
        }
        this.serializer = instantiateClass(serializerClass);
        if (this.serializer != null) {
            this.serializer.configure(context);
        }
    }

    private <T> T instantiateClass(String className) {
        try {
            @SuppressWarnings("unchecked")
            Class<T> aClass = (Class<T>) Class.forName(className);
            return aClass.newInstance();
        } catch (Exception e) {
            logger.error("Could not instantiate class " + className, e);
            Throwables.propagate(e);
            return null;
        }
    }

    /**
     * returns hosts
     */
    private String[] getHosts(Context context) {
        String[] hosts = null;
        if (StringUtils.isNotBlank(context.getString(ES_HOSTS))) {
            hosts = context.getString(ES_HOSTS).split(",");
        }
        return hosts;
    }

    /**
     * Checks for elasticsearch connection
     * Sets shouldBackOff to true if bulkProcessor failed to deliver the request.
     * Resets shouldBackOff to false once the connection to elasticsearch is established.
     */
    public void assertConnection() {
        shouldBackOff.set(true);
        final Timer timer = new Timer();
        final TimerTask task = new TimerTask() {
            @Override
            public void run() {
                try {
                    if (checkConnection()) {
                        shouldBackOff.set(false);
                        timer.cancel();
                        timer.purge();
                    }
                } catch (IOException e) {
                    logger.error("ping request for elasticsearch failed " + e.getMessage(), e);
                }
            }
        };
        timer.scheduleAtFixedRate(task, 0, CHECK_CONNECTION_PERIOD);
    }

    private boolean checkConnection() throws IOException {
        return client.ping(RequestOptions.DEFAULT);
    }
}
