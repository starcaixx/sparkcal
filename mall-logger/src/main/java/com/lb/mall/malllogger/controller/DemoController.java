package com.lb.mall.malllogger.controller;

import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class DemoController {


    @RequestMapping("testDemo")
    public String testDemo() {
        return "hello demo";
    }
}
