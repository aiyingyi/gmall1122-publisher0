package com.atguigu.gmall1122.publisher0.service;

import java.util.Map;

public interface EsService {

    //日活的总数查询
    public Long getDauTotal(String date);

    //日活的分时查询
    public Map getDauHour(String date);
}
