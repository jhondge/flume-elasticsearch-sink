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

import com.google.gson.JsonParser;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.event.EventBuilder;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.charset.Charset;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

import static com.cognitree.flume.sink.elasticsearch.Constants.*;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.junit.Assert.assertEquals;

public class TestServiceLogSerializer {

    private NginxLogSerializer nginxSerializer;

    private static final Charset charset = Charset.defaultCharset();

    @Before
    public void init() {
        nginxSerializer = new NginxLogSerializer();
    }

    /**
     * tests csv serializer
     */
    @Test
    public void testSerializer() throws Exception {
        Context context = new Context();
        String type = "datetime:date,thread_id:string,log_level:string,log_class:string,request_id:string,log_message:string";
        String regex = "(.*)\\s+\\[([^\\s]*)\\]\\s([^\\s]*)\\s+(.*)\\s-\\s\\[(.*)\\]\\s(.*)";

//        String message = "172.16.0.65 - - [15/Mar/2019:00:22:11 +0000] \"GET /api/userinfo/power?country=CN&appVersion=0.9.2&osVersion=28&timezone=Asia%2FHong_Kong" +
//                "&appName=%E5%83%8F%E7%B4%A0%E8%9C%9C%E8%9C%82&language=zh&deviceName=ONEPLUS%20A5000&platform=1&" +
//                "timestamp=1552609335079&signature=b528fd9ca445ecab66387dbb29f9a6c0 HTTP/1.1\" \"-\" 200 214 \"-\" \"okhttp/3.10.0\" \"112.96.100.96\" 10.247.255.27:8080 0.047 0.048";

        String me = "2019-04-07 03:16:27.345 [http-nio-8088-exec-14] INFO  c.everimaging.pixbewebsocket.aop.BusinessLogAspect - [B032A66CD53B408EA0945BA8D9BA01CD] > module:websocket, ip:172.16.0.81, ua:\\\"Apache-HttpClient/4.5.2 (Java 1.5 minimum; Java/1.8.0_191)\\\", token:\\\"null\\\", method:GET, uri:/api/socket-open, body:account=p55452087";
        String message = "{\"log\":\"" + me + "\"}";

//        message = "[2016-01-08T15:33:55+08:00]";

        String dateformat = "yyyy-MM-dd HH:mm:ss.SSS";
//
//        regex = "\\\\[(.*)\\\\]";
        context.put(ES_NGINX_LOG_FIELDS, type);
        context.put(ES_NGINX_LOG_REGEX, regex);
        context.put(ES_NGINX_LOG_DATEFORMAT, dateformat);
        context.put(ES_NGINX_LOG_DATEFORMAT_TIMEZONE, "GMT+8");

        nginxSerializer.configure(context);

        Event event = EventBuilder.withBody(message.getBytes(charset));
        XContentBuilder expected = generateContentBuilder();
        XContentBuilder actual = nginxSerializer.serialize(event);
        JsonParser parser = new JsonParser();
        assertEquals(parser.parse(Strings.toString(expected)), parser.parse(Strings.toString(actual)));
    }

    private XContentBuilder generateContentBuilder() throws IOException, ParseException {
        XContentBuilder expected = jsonBuilder().startObject();

        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
        simpleDateFormat.setTimeZone(TimeZone.getTimeZone("GMT+8"));

        Date date = simpleDateFormat.parse("2019-04-07 03:16:27.345");
        System.out.println("timezone:" + simpleDateFormat.getTimeZone() + ", timestamp:" + date.getTime());
        expected.field("datetime", date);
        expected.field("thread_id", "http-nio-8088-exec-14");
        expected.field("log_level", "INFO");
        expected.field("log_class", "c.everimaging.pixbewebsocket.aop.BusinessLogAspect");
        expected.field("request_id", "B032A66CD53B408EA0945BA8D9BA01CD");
        expected.field("log_message", "> module:websocket, ip:172.16.0.81, ua:\"Apache-HttpClient/4.5.2 (Java 1.5 minimum; Java/1.8.0_191)\", token:\"null\", method:GET, uri:/api/socket-open, body:account=p55452087");

        expected.endObject();

        return expected;
    }
}
