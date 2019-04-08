package com.cognitree.flume.sink.elasticsearch.handler;

import org.apache.http.client.HttpClient;
import org.apache.http.impl.client.HttpClients;
import org.elasticsearch.common.xcontent.XContentBuilder;

/**
 * @author Jackson
 * @date 2019-03-18 6:30 PM
 * @email <a href="mailto:zhangjiajun@everimaging.com">zhangjiajun@everimaging.com</a>
 */
public class OAuth2ValueHandler implements ValueHandler {

    private String endpoint;

    private HttpClient client;

    public OAuth2ValueHandler(String endpoint){
        client = HttpClients.createDefault();
        this.endpoint = endpoint;
    }

    @Override
    public void process(XContentBuilder xContentBuilder, String key, String value) {



    }
}
