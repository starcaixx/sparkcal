package com.longbei.bigdata;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

public class LogInterceptor implements Interceptor {
    public static class Builder implements Interceptor.Builder{

        @Override
        public Interceptor build() {
            return new LogInterceptor();
        }

        @Override
        public void configure(Context context) {

        }
    }
    @Override
    public void initialize() {

    }

    @Override
    public Event intercept(Event event) {
        // 1 获取数据
        byte[] body = event.getBody();
        String log = new String(body, Charset.forName("UTF-8"));

        // 2 判断数据类型并向Header中赋值
        String[] logs = log.split("\t");
        if (logs.length==10 && logs[9] != null && !"".equals(logs[9])) {
                return event;
        }

        // 3 返回校验结果
        return null;
    }

    @Override
    public List<Event> intercept(List<Event> list) {
        ArrayList<Event> interceptors = new ArrayList<>();

        for (Event event : list) {
            Event intercept1 = intercept(event);

            if (intercept1 != null){
                interceptors.add(intercept1);
            }
        }
        return interceptors;
    }

    @Override
    public void close() {

    }
}
