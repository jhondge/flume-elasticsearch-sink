package com.cognitree.flume.sink.elasticsearch;

import com.google.common.base.Charsets;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.flume.Context;
import org.apache.flume.Event;

import java.util.Map;
import java.util.regex.MatchResult;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author Jackson
 * @date 2019-03-18 9:42 AM
 * @email <a href="mailto:zhangjiajun@everimaging.com">zhangjiajun@everimaging.com</a>
 */
public class NginxIndexBuilder extends StaticIndexBuilder{

    private Pattern variableRegex;

    public NginxIndexBuilder() {
        this.variableRegex = Pattern.compile("\\$\\{([^\\$\\{\\}]*)\\}");
    }

    @Override
    public String getId(Event event) {
        JsonParser parser = new JsonParser();
        String body = new String(event.getBody(), Charsets.UTF_8);
        JsonElement element = parser.parse(body);
        JsonObject jsonObject = element.getAsJsonObject();
        String timeStr = jsonObject.get("time").getAsString();
        return timeStr;
    }

    @Override
    public String getIndex(Event event) {
        Matcher indexMatcher = this.variableRegex.matcher(this.index);

        while (indexMatcher.find() && event.getHeaders() != null ){
            Map<String, String> headers = event.getHeaders();
            for (int group = 0, count = indexMatcher.groupCount(); group < count; group++) {
                int groupIndex = group + 1;
                if (groupIndex > count) {
                    break;
                }
                String variable = indexMatcher.group(groupIndex);
                String value = headers.get(variable);
                String regex = "\\$\\{" + variable+ "\\}";
                Pattern pattern = Pattern.compile(regex);
                Matcher matcher = pattern.matcher(this.index);
                this.index = matcher.replaceAll(value);
            }
        }
        return super.getIndex(event);
    }

    @Override
    public void configure(Context context) {
        super.configure(context);
    }
}
