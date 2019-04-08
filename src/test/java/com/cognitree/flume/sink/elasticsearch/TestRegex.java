package com.cognitree.flume.sink.elasticsearch;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.junit.Assert;
import org.junit.Test;

import java.util.regex.Pattern;

/**
 * @author Jackson
 * @date 2019-04-08 10:43 PM
 * @email <a href="mailto:zhangjiajun@everimaging.com">zhangjiajun@everimaging.com</a>
 */
public class TestRegex {


    @Test
    public void testPrefix(){
        String logMessage = "2019-04-07 03:16:27.345 [http-nio-8088-exec-14] INFO  c.everimaging.pixbewebsocket.aop.BusinessLogAspect - [B032A66CD53B408EA0945BA8D9BA01CD] < module:websocket, result:[code=000, data=false]";

        String regex = "^\\d\\d\\d\\d-\\d+-\\d+\\s\\d+:\\d+\\d:\\d+";

        Pattern pattern = Pattern.compile(regex);

        Assert.assertTrue(pattern.matcher(logMessage).find());

    }

    @Test
    public void testAppendJson(){

        String me = "2019-04-07 03:16:27.345 [http-nio-8088-exec-14] INFO  c.everimaging.pixbewebsocket.aop.BusinessLogAspect - [B032A66CD53B408EA0945BA8D9BA01CD] > module:websocket, ip:172.16.0.81, ua:\\\"Apache-HttpClient/4.5.2 (Java 1.5 minimum; Java/1.8.0_191)\\\", token:\\\"null\\\", method:GET, uri:/api/socket-open, body:account=p55452087";
        String message = "{\"log\":\"" + me + "\"}";


        JsonParser parser = new JsonParser();
        JsonElement element = parser.parse(message);
        JsonObject jsonObject = element.getAsJsonObject();
        String logStr = jsonObject.get("log").getAsString();

        jsonObject.addProperty("log", logStr + "|||||end");


        Assert.assertEquals("{\"log\":\"" + me + "|||||end\"}", jsonObject.toString());
    }

}
