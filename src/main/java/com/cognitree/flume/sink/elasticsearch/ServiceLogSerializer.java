package com.cognitree.flume.sink.elasticsearch;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * @author Jackson
 * @date 2019-04-08 6:36 PM
 * @email <a href="mailto:zhangjiajun@everimaging.com">zhangjiajun@everimaging.com</a>
 */
public class ServiceLogSerializer extends NginxLogSerializer{

    private static final Logger logger = LoggerFactory.getLogger(ServiceLogSerializer.class);

    @Override
    public XContentBuilder serialize(Event event) {
        return super.serialize(event);
    }

    @Override
    public void configure(Context context) {
        super.configure(context);
    }
}
