package com.lb.bigdata.publisher.service;

import org.springframework.stereotype.Service;

@Service
public interface EsService {
    public Long getDauTotal(String date);
}
