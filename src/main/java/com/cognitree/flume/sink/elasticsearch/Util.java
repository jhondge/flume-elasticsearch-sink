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

import org.apache.commons.lang.StringUtils;
import org.apache.flume.Context;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import static com.cognitree.flume.sink.elasticsearch.Constants.*;

public class Util {

    private static final Logger logger = LoggerFactory.getLogger(Util.class);

    /**
     * Returns TimeValue based on the given interval
     * Interval can be in minutes, seconds, mili seconds
     */
    public static TimeValue getTimeValue(String interval, String defaultValue) {
        TimeValue timeValue = null;
        String timeInterval = interval != null ? interval : defaultValue;
        logger.trace("Time interval is [{}] ", timeInterval);
        if (timeInterval != null) {
            Integer time = Integer.valueOf(timeInterval.substring(0, timeInterval.length() - 1));
            String unit = timeInterval.substring(timeInterval.length() - 1);
            UnitEnum unitEnum = UnitEnum.fromString(unit);
            switch (unitEnum) {
                case MINUTE:
                    timeValue = TimeValue.timeValueMinutes(time);
                    break;
                case SECOND:
                    timeValue = TimeValue.timeValueSeconds(time);
                    break;
                case MILI_SECOND:
                    timeValue = TimeValue.timeValueMillis(time);
                    break;
                default:
                    logger.error("Unit is incorrect, please check the Time Value unit: " + unit);
            }
        }
        return timeValue;
    }

    /**
     * Returns ByteSizeValue of the given byteSize and unit
     * byteSizeUnit can be in Mega bytes, Kilo Bytes
     */
    public static ByteSizeValue getByteSizeValue(Integer byteSize, String unit) {
        ByteSizeValue byteSizeValue = new ByteSizeValue(DEFAULT_ES_BULK_SIZE, ByteSizeUnit.MB);
        logger.trace("Byte size value is [{}] ", byteSizeValue);
        if (byteSize != null && unit != null) {
            ByteSizeEnum byteSizeEnum = ByteSizeEnum.valueOf(unit.toUpperCase());
            switch (byteSizeEnum) {
                case MB:
                    byteSizeValue = new ByteSizeValue(byteSize, ByteSizeUnit.MB);
                    break;
                case KB:
                    byteSizeValue = new ByteSizeValue(byteSize, ByteSizeUnit.KB);
                    break;
                default:
                    logger.error("Unit is incorrect, please check the Byte Size unit: " + unit);
            }
        }
        return byteSizeValue;
    }

    /**
     * Returns the context value for the contextId
     */
    public static String getContextValue(Context context, String contextId) {
        String contextValue = null;
        if (StringUtils.isNotBlank(context.getString(contextId))) {
            contextValue = context.getString(contextId);
        }
        return contextValue;
    }

    public static void addField(XContentBuilder xContentBuilder, String key, String value, String type) throws IOException{
        addField(xContentBuilder, key, value, type, null);
    }

    /**
     * Add csv field to the XContentBuilder
     */
    public static void addField(XContentBuilder xContentBuilder, String key, String value, String type, SimpleDateFormat dateFormat) throws IOException {
        if (type != null) {
            FieldTypeEnum fieldTypeEnum = FieldTypeEnum.valueOf(type.toUpperCase());
            switch (fieldTypeEnum) {
                case STRING:
                    xContentBuilder.field(key, value);
                    break;
                case FLOAT:
                    float fvalue = 0.00f;
                    try{
                        fvalue = Float.valueOf(value);
                    } catch (NumberFormatException e){
                        logger.error("parse float value error, key[{}], value[{}]", new Object[]{key, value});
                    }
                    xContentBuilder.field(key, fvalue);
                    break;
                case INT:
                    float ivalue = 0;
                    try{
                        ivalue = Integer.valueOf(value);
                    } catch (NumberFormatException e){
                        logger.error("parse integer value error, key[{}], value[{}]", new Object[]{key, value});
                    }
                    xContentBuilder.field(key, ivalue);
                    break;
                case LONG:
                    float lvalue = 0;
                    try{
                        lvalue = Long.parseLong(value);
                    } catch (NumberFormatException e){
                        logger.error("parse long value error, key[{}], value[{}]", new Object[]{key, value});
                    }
                    xContentBuilder.field(key, lvalue);
                    break;
                case BOOLEAN:
                    xContentBuilder.field(key, Boolean.valueOf(value));
                    break;
                case DATE:
                    Date date = null;
                    if(dateFormat != null) {
                        try {
                            date = dateFormat.parse(value);
                        } catch (ParseException e) {
                            e.printStackTrace();
                        }
                    }
                    xContentBuilder.field(key, date);
                    break;

                default:
                    logger.error("Type is incorrect, please check type: " + type);
            }
        }
    }
}
