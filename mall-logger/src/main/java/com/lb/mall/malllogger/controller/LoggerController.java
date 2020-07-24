package com.lb.mall.malllogger.controller;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@Controller  // 返回页面
@RestController  //返回字符串值       == @Controller +方法上的@ResponseBody
@Slf4j
public class LoggerController {

    @Autowired
    KafkaTemplate kafkaTemplate;

    @RequestMapping("/applog")
    public String applog(@RequestBody String str/*, @RequestParam("log") String str*/) {
        log.info(str);
        JSONObject jsonObject = JSON.parseObject(str);
        if (jsonObject.getString("start") != null && !"".equals(jsonObject.getString("start"))) {
            kafkaTemplate.send("gmall_start",str);
        }else {
            kafkaTemplate.send("gmall_envent",str);
        }
        return "success";
    }
}
