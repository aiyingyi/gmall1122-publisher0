package com.atguigu.gmall1122.publisher0.controller;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall1122.publisher0.service.EsService;
import org.apache.commons.lang3.time.DateUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

@RestController
public class PublisherController {


    @Autowired
    EsService  esService;

    // @RequestMapping(value = "realtime-total",method = RequestMethod.GET)
    @GetMapping("realtime-total") // 通过get请求方式才会响应
    public String realtimeTotal(@RequestParam("date") String dt){

        ArrayList<Map> totalList = new ArrayList<>();
        // 定义一个Map，用于存储一个JSON对象的数据
        HashMap<Object, Object> dauMap = new HashMap<>();
        dauMap.put("id","dau");
        Long dauTotal = esService.getDauTotal(dt);
        dauMap.put("value",dauTotal);

        // 将json对象的数据放入列表，一个List对应于一个json对象数组
        totalList.add(dauMap);

        HashMap newMidMap = new HashMap<>();
        newMidMap.put("id","dau");
        newMidMap.put("name","新增设备");
        newMidMap.put("value",233);
        totalList.add(newMidMap);

        // 将List<Map>转换成JSON数组格式
        return JSON.toJSONString(totalList);

    }

    // 返回今天和昨天的按照每小时聚合的数据
    @GetMapping("realtime-hour")
    public String realtimeHour(@RequestParam(value = "id",defaultValue ="-1" ) String id ,@RequestParam("date") String dt ){
        if(id.equals("dau")){
            // Map的嵌套，用于定义嵌套的json对象
            Map<String,Map> hourMap=new HashMap<>();
            Map dauHourTdMap = esService.getDauHour(dt);
            hourMap.put("today",dauHourTdMap);
            // 获取昨天的日期
            String yd = getYd(dt);
            Map dauHourYdMap = esService.getDauHour(yd);
            // 放入昨天的数据
            hourMap.put("yesterday",dauHourYdMap);
            return JSON.toJSONString(hourMap);
        }
        return null;
    }

    // 根据日期字符串获取昨天的日期
    private  String getYd(String td){
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
        String yd=null;
        try {
            Date tdDate = dateFormat.parse(td);
            // DateUtils工具类在lang3包里面
            Date ydDate = DateUtils.addDays(tdDate, -1);
            yd=dateFormat.format(ydDate);
        } catch (ParseException e) {
            e.printStackTrace();
            throw new RuntimeException("日期格式转变失败");
        }
        return yd;
    }


    @GetMapping("test")
    public String test(){
        return "hello world";
    }

    @GetMapping("area")
    public String area(){
        List list=new ArrayList();
        JSONObject hb = new JSONObject();
        hb.put("area_id","420000" );
        hb.put("value",500000 );
        JSONObject hlj = new JSONObject();
        hlj.put("area_id","230000" );
        hlj.put("value",300000 );

        list.add(hb);
        list.add(hlj);
        return JSON.toJSONString(list);
    }



}
