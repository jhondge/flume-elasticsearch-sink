package com.cognitree.flume.sink.elasticsearch;

import com.google.common.base.Charsets;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.flume.Event;

/**
 * @author Jackson
 * @date 2019-03-18 9:42 AM
 * @email <a href="mailto:zhangjiajun@everimaging.com">zhangjiajun@everimaging.com</a>
 */
public class NginxIndexBuilder extends StaticIndexBuilder{

    @Override
    public String getId(Event event) {
        JsonParser parser = new JsonParser();
        String body = new String(event.getBody(), Charsets.UTF_8);
        JsonElement element = parser.parse(body);
        JsonObject jsonObject = element.getAsJsonObject();
        String timeStr = jsonObject.get("time").getAsString();
        return timeStr;
    }
}
