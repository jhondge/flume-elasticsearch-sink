package com.cognitree.flume.sink.elasticsearch.handler;

import org.elasticsearch.common.xcontent.XContentBuilder;

/**
 * @author Jackson
 * @date 2019-03-18 6:23 PM
 * @email <a href="mailto:zhangjiajun@everimaging.com">zhangjiajun@everimaging.com</a>
 */
public interface ValueHandler {

    /**
     *
     * @param xContentBuilder
     * @param key
     * @param value
     */
    void process(XContentBuilder xContentBuilder, String key, String value);

}
