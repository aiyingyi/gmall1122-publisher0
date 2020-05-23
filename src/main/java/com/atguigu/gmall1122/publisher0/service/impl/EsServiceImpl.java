package com.atguigu.gmall1122.publisher0.service.impl;


import com.atguigu.gmall1122.publisher0.service.EsService;
import io.searchbox.client.JestClient;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;
import io.searchbox.core.search.aggregation.TermsAggregation;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


@Service
public class EsServiceImpl implements EsService {
    @Autowired
    JestClient jestClient;
    @Override
    public Long getDauTotal(String date) {
        // 创建查询的字符串
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(new MatchAllQueryBuilder());
        String query = searchSourceBuilder.toString();

        // 转换日期结构
        date=date.replace("-","");
        String indexName="gmall1122_dau_info_"+date+"-query";
        // 创建查询
        Search search = new Search.Builder(query).addIndex(indexName).addType("_doc").build();
        Long total = 0L;
        try {
            SearchResult searchResult = jestClient.execute(search);
            if(searchResult.getTotal() != null){
                total = searchResult.getTotal();
            }
        } catch (IOException e) {
            e.printStackTrace();
            throw  new RuntimeException("查询ES异常");
        }

        return total;
    }

    @Override
    public Map getDauHour(String date) {

        // 拼接查询的字符串
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        // aggregationBuilder: 聚合操作对应的builder
        TermsAggregationBuilder aggregationBuilder = AggregationBuilders.terms("groupby_hr").field("hr").size(24);
        searchSourceBuilder.aggregation(aggregationBuilder);

        String query = searchSourceBuilder.toString();

        date=date.replace("-","");
        String indexName="gmall1122_dau_info_"+date+"-query";

        Search search = new Search.Builder(query).addIndex(indexName).addType("_doc").build();
        Map aggsMap=new HashMap();
        try {
            SearchResult searchResult = jestClient.execute(search);
            if(searchResult.getAggregations().getTermsAggregation("groupby_hr")!=null){
                // 获取聚合的结果，放入到List中
                List<TermsAggregation.Entry> buckets = searchResult.getAggregations().getTermsAggregation("groupby_hr").getBuckets();
                for (TermsAggregation.Entry bucket : buckets) {

                    // 将结果放入到map中，作为json字符串返回
                    aggsMap.put(  bucket.getKey(),bucket.getCount());
                }
            }

        } catch (IOException e) {
            e.printStackTrace();
            throw  new RuntimeException("查询ES异常");
        }

        // 一个Map对应于一个JSON对象
        return aggsMap;
    }
}
