package com.cognitree.flume.sink.elasticsearch;

import com.google.common.base.Charsets;
import com.google.common.base.Throwables;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.RegexExtractorInterceptor;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.cognitree.flume.sink.elasticsearch.Constants.*;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

/**
 * @author Jackson
 * @date 2019-03-15 10:31 AM
 * @email <a href="mailto:zhangjiajun@everimaging.com">zhangjiajun@everimaging.com</a>
 */
public class NginxLogSerializer implements Serializer {

    private static final Logger logger = LoggerFactory.getLogger(NginxLogSerializer.class);

    private final List<String> names = new ArrayList<String>();

    private final List<String> types = new ArrayList<String>();

    private Pattern regex;

    @Override
    public XContentBuilder serialize(Event event) {
        XContentBuilder xContentBuilder = null;
        String body = new String(event.getBody(), Charsets.UTF_8);
        try {

            //{"log": xx, "steam"}
            JsonParser parser = new JsonParser();
            JsonElement element = parser.parse(body);
            JsonObject jsonObject = element.getAsJsonObject();
            String logStr = jsonObject.get("log").getAsString();

            Matcher matcher = regex.matcher(logStr);
            if (matcher.find()) {
                xContentBuilder = jsonBuilder().startObject();
                for (int group = 0, count = matcher.groupCount(); group < count; group++) {
                    int groupIndex = group + 1;
                    if (groupIndex > names.size()) {
                        if (logger.isDebugEnabled()) {
                            logger.debug("Skipping group {} to {} due to missing serializer",
                                    group, count);
                        }
                        break;
                    }
                    String value = matcher.group(groupIndex);
                    Util.addField(xContentBuilder, names.get(group), value, types.get(group));
                }
                xContentBuilder.endObject();
            }

        } catch (Exception e) {
            logger.error("Error in converting the body to the json format " + e.getMessage(), e);
        }
        return xContentBuilder;
    }

    @Override
    public void configure(Context context) {
        String fields = context.getString(ES_NGINX_LOG_FIELDS);
        if (fields == null) {
            Throwables.propagate(new Exception("Fields for nginx log files are not configured," +
                    " please configured the property " + ES_NGINX_LOG_FIELDS));
        }
        String regexString = context.getString(ES_NGINX_LOG_REGEX);

        regex = Pattern.compile(regexString);
        regex.pattern();
        regex.matcher("").groupCount();
        try {
//            delimiter = context.getString(ES_CSV_DELIMITER, DEFAULT_ES_CSV_DELIMITER);
            String[] fieldTypes = fields.split(COMMA);
            for (String fieldType : fieldTypes) {
                names.add(getValue(fieldType, 0));
                types.add(getValue(fieldType, 1));
            }
        } catch (Exception e) {
            Throwables.propagate(e);
        }
    }

    /**
     * Returns name and value based on the index
     */
    private String getValue(String fieldType, Integer index) {
        String value = "";
        if (fieldType.length() > index) {
            value = fieldType.split(COLONS)[index];
        }
        return value;
    }
}
