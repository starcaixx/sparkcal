package com.lb.bigdata.publisher.controller;

import com.alibaba.fastjson.JSON;
import com.lb.bigdata.publisher.service.EsService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

@RestController
public class PublisherController {

    @Autowired
    EsService esService;

    @GetMapping("realtime-total")
    public String realtimeTotal(@RequestParam("date") String dt) {
        HashMap<String, String> dauMap = new HashMap<>();

        dauMap.put("id","dau");
        dauMap.put("name","add");
        Long total = esService.getDauTotal(dt);
        dauMap.put("value",total+"");
        ArrayList<Map> list = new ArrayList<>();
        list.add(dauMap);
        return JSON.toJSONString(list);
    }
}
