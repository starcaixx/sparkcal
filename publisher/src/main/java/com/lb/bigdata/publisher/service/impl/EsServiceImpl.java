package com.lb.bigdata.publisher.service.impl;

import com.lb.bigdata.publisher.service.EsService;
import io.searchbox.client.JestClient;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;

@Service
public class EsServiceImpl implements EsService {

    @Autowired
    JestClient jestClient;
    @Override
    public Long getDauTotal(String date) {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(new MatchAllQueryBuilder());

        String query = searchSourceBuilder.toString();
        String indexName = "gmall_dau_info_"+date;
        Search search = new Search.Builder(query).addIndex(indexName).addType("_doc").build();
        Long total = 0l;
        try {
            SearchResult result = jestClient.execute(search);
            total = result.getTotal();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return total;
    }
}
