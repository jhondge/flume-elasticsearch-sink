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
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.script.mustache.SearchTemplateRequest;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.charset.Charset;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import static com.cognitree.flume.sink.elasticsearch.Constants.*;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.junit.Assert.assertEquals;

public class TestNinxLogSerializer {

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
        String type = "remote_addr:string,remote_user:string,datetime:date,http_method:string,uri:string,request_body:string,status:int,body_length:long,http_referer:string,user_agent:string,http_x_forwarded_for:string,upstream_addr:string,upstream_response_time:float,request_time:float";
//        String regex = "([^\\\\s]*)\\\\s-\\\\s([^\\\\s]*)\\\\s\\\\[(.*)\\\\]\\\\s+\\\\\"([\\\\S]*)\\\\s+([\\\\S]*)\\\\s+[\\\\S]*\\\\\"\\\\s+\\\\\"([^\\\\\"]*)\\\\\"\\\\s+(\\\\d+)\\\\s+(\\\\d+)\\\\s+\\\\\"([^\\\\\"]*)\\\\\"\\\\s+\\\\\"([^\\\\\"]*)\\\\\"\\\\s+\\\\\"([^\\\\\"]*)\\\\\"\\\\s+([^\\\\s]*)\\\\s+([^\\\\s]*)\\\\s+([^\\\\]*)";
        String regex = "([^\\\\s]*)\\\\s-\\\\s([^\\\\s]*)\\\\s\\\\[(.*)\\\\]\\\\s+\\\\\"([\\\\S]*)\\\\s+([\\\\S]*)\\\\s+[\\\\S]*\\\\\"\\\\s+\\\\\"([^\\\\\"]*)\\\\\"\\\\s+(\\\\d+)\\\\s+(\\\\d+)\\\\s+\\\\\"([^\\\\\"]*)\\\\\"\\\\s+\\\\\"([^\\\\\"]*)\\\\\"\\\\s+\\\\\"([^\\\\\"]*)\\\\\"\\\\s+([^\\\\s]*)\\\\s+([^\\\\s]*)\\\\s+([^\\\\]*)";

        regex = "([^\\s]*)\\s-\\s([^\\s]*)\\s\\[(.*)\\]\\s+\\\"([\\S]*)\\s+([\\S]*)\\s+[\\S]*\\\"\\s+\\\"([^\\\"]*)\\\"\\s+(\\d+)\\s+(\\d+)\\s+\\\"([^\\\"]*)\\\"\\s+\\\"([^\\\"]*)\\\"\\s+\\\"([^\\\"]*)\\\"\\s+([^\\s]*)\\s+([^\\s]*)\\s+([^\\s]*)";

        String message = "172.16.0.65 - - [15/Mar/2019:00:22:11 +0000] \"GET /api/userinfo/power?country=CN&appVersion=0.9.2&osVersion=28&timezone=Asia%2FHong_Kong" +
                "&appName=%E5%83%8F%E7%B4%A0%E8%9C%9C%E8%9C%82&language=zh&deviceName=ONEPLUS%20A5000&platform=1&" +
                "timestamp=1552609335079&signature=b528fd9ca445ecab66387dbb29f9a6c0 HTTP/1.1\" \"-\" 200 214 \"-\" \"okhttp/3.10.0\" \"112.96.100.96\" 10.247.255.27:8080 0.047 0.048";

        message = "{\"log\":\"172.16.0.97 - - [15/Mar/2019:10:00:23 +0000] \\\"GET /api/posts/follows-by-createTime?createTime=-1&pageSize=12&country=CN&appVersion=0.9.2&osVersion=27&timezone=Asia%2FShanghai&appName=%E5%83%8F%E7%B4%A0%E8%9C%9C%E8%9C%82&language=zh&deviceName=OPPO%20R11&platform=1&timestamp=1552835567528&signature=a4a8797e763589ef48d084caca1f0bf3 HTTP/1.1\\\" \\\"-\\\" 200 393 \\\"-\\\" \\\"kube-probe/1.9+\\\" \\\"-\\\" - - 0.000\\n\",\"stream\":\"stdout\",\"time\":\"2019-03-15T10:00:23.680580284Z\"}";

//        message = "[2016-01-08T15:33:55+08:00]";

        String dateformat = "dd/MMM/yyy:HH:mm:ss Z";
//
//        regex = "\\\\[(.*)\\\\]";
        context.put(ES_NGINX_LOG_FIELDS, type);
        context.put(ES_NGINX_LOG_REGEX, regex);
        context.put(ES_NGINX_LOG_DATEFORMAT, dateformat);

        nginxSerializer.configure(context);

        Event event = EventBuilder.withBody(message.getBytes(charset));
        XContentBuilder expected = generateContentBuilder();
        XContentBuilder actual = nginxSerializer.serialize(event);
        JsonParser parser = new JsonParser();
        assertEquals(parser.parse(Strings.toString(expected)), parser.parse(Strings.toString(actual)));
    }

    private XContentBuilder generateContentBuilder() throws IOException, ParseException {
        XContentBuilder expected = jsonBuilder().startObject();
        expected.field("remote_addr", "172.16.0.97");
        expected.field("remote_user", "-");

        Date date = new SimpleDateFormat("dd/MMM/yyy:HH:mm:ss Z").parse("15/Mar/2019:10:00:23 +0000");
        expected.field("datetime", date);
        expected.field("http_method", "GET");
        expected.field("uri", "/api/posts/follows-by-createTime");
        expected.field("uri_params", "createTime=-1&pageSize=12&country=CN&appVersion=0.9.2&osVersion=27&timezone=Asia%2FShanghai&appName=%E5%83%8F%E7%B4%A0%E8%9C%9C%E8%9C%82&language=zh&deviceName=OPPO%20R11&platform=1&timestamp=1552835567528&signature=a4a8797e763589ef48d084caca1f0bf3");
        expected.field("request_body", "-");
        expected.field("status", 200);
        expected.field("body_length", 393);
        expected.field("http_referer", "-");
        expected.field("user_agent", "kube-probe/1.9+");
        expected.field("http_x_forwarded_for", "-");
        expected.field("upstream_addr", "-");
        expected.field("upstream_response_time", 0);
        expected.field("request_time", 0);
        expected.endObject();
        return expected;
    }
}
