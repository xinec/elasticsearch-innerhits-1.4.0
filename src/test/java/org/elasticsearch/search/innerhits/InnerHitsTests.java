/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.search.innerhits;

import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.fetch.innerhits.InnerHitsBuilder;
import org.elasticsearch.search.internal.InternalSearchHit;
import org.elasticsearch.search.internal.InternalSearchHits;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.*;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

/**
 */
public class InnerHitsTests extends ElasticsearchIntegrationTest {

    @Test
    public void testSimpleNested() throws Exception {
        assertAcked(prepareCreate("articles").addMapping("article", jsonBuilder().startObject().startObject("article").startObject("properties")
                .startObject("comments")
                .field("type", "nested")
                .startObject("properties")
                .startObject("message")
                .field("type", "string")
                .endObject()
                .endObject()
                .endObject()
                .startObject("title")
                .field("type", "string")
                .endObject()
                .endObject().endObject().endObject()));

        List<IndexRequestBuilder> requests = new ArrayList<>();
        requests.add(client().prepareIndex("articles", "article", "1").setSource(jsonBuilder().startObject()
                .field("title", "quick brown fox")
                .startArray("comments")
                .startObject().field("message", "fox eat quick").endObject()
                .startObject().field("message", "fox ate rabbit x y z").endObject()
                .startObject().field("message", "rabbit got away").endObject()
                .endArray()
                .endObject()));
        requests.add(client().prepareIndex("articles", "article", "2").setSource(jsonBuilder().startObject()
                .field("title", "big gray elephant")
                .startArray("comments")
                .startObject().field("message", "elephant captured").endObject()
                .startObject().field("message", "mice squashed by elephant x").endObject()
                .startObject().field("message", "elephant scared by mice x y").endObject()
                .endArray()
                .endObject()));
        indexRandom(true, requests);

        SearchResponse response = client().prepareSearch("articles")
                .setQuery(QueryBuilders.nestedQuery("comments", QueryBuilders.matchQuery("comments.message", "fox")))
                .addInnerHit("comment", new InnerHitsBuilder.InnerHit().setPath("comments").setQuery(QueryBuilders.matchQuery("comments.message", "fox")))
                .get();

        assertNoFailures(response);
        assertHitCount(response, 1);
        assertSearchHit(response, 1, hasId("1"));
        assertThat(response.getHits().getAt(0).getInnerHits().size(), equalTo(1));
        SearchHits innerHits = response.getHits().getAt(0).getInnerHits().get("comment");
        assertThat(innerHits.totalHits(), equalTo(2l));
        assertThat(innerHits.getHits().length, equalTo(2));
        assertThat(innerHits.getAt(0).getId(), equalTo("1"));
        assertThat(innerHits.getAt(0).getNestedIdentity().getField().string(), equalTo("comments"));
        assertThat(innerHits.getAt(0).getNestedIdentity().getOffset(), equalTo(0));
        assertThat(innerHits.getAt(1).getId(), equalTo("1"));
        assertThat(innerHits.getAt(1).getNestedIdentity().getField().string(), equalTo("comments"));
        assertThat(innerHits.getAt(1).getNestedIdentity().getOffset(), equalTo(1));

        response = client().prepareSearch("articles")
                .setQuery(QueryBuilders.nestedQuery("comments", QueryBuilders.matchQuery("comments.message", "elephant")))
                .addInnerHit("comment", new InnerHitsBuilder.InnerHit().setPath("comments").setQuery(QueryBuilders.matchQuery("comments.message", "elephant")))
                .get();

        assertNoFailures(response);
        assertHitCount(response, 1);
        assertSearchHit(response, 1, hasId("2"));
        assertThat(response.getHits().getAt(0).getInnerHits().size(), equalTo(1));
        innerHits = response.getHits().getAt(0).getInnerHits().get("comment");
        assertThat(innerHits.totalHits(), equalTo(3l));
        assertThat(innerHits.getHits().length, equalTo(3));
        assertThat(innerHits.getAt(0).getId(), equalTo("2"));
        assertThat(innerHits.getAt(0).getNestedIdentity().getField().string(), equalTo("comments"));
        assertThat(innerHits.getAt(0).getNestedIdentity().getOffset(), equalTo(0));
        assertThat(innerHits.getAt(1).getId(), equalTo("2"));
        assertThat(innerHits.getAt(1).getNestedIdentity().getField().string(), equalTo("comments"));
        assertThat(innerHits.getAt(1).getNestedIdentity().getOffset(), equalTo(1));
        assertThat(innerHits.getAt(2).getId(), equalTo("2"));
        assertThat(innerHits.getAt(2).getNestedIdentity().getField().string(), equalTo("comments"));
        assertThat(innerHits.getAt(2).getNestedIdentity().getOffset(), equalTo(2));
    }

    @Test
    public void testSimpleParentChild() throws Exception {
        assertAcked(prepareCreate("articles")
                        .addMapping("article", "title", "type=string")
                        .addMapping("comment", "_parent", "type=article", "message", "type=string")
        );

        List<IndexRequestBuilder> requests = new ArrayList<>();
        requests.add(client().prepareIndex("articles", "article", "1").setSource("title", "quick brown fox"));
        requests.add(client().prepareIndex("articles", "comment", "1").setParent("1").setSource("message", "fox eat quick"));
        requests.add(client().prepareIndex("articles", "comment", "2").setParent("1").setSource("message", "fox ate rabbit x y z"));
        requests.add(client().prepareIndex("articles", "comment", "3").setParent("1").setSource("message", "rabbit got away"));
        requests.add(client().prepareIndex("articles", "article", "2").setSource("title", "big gray elephant"));
        requests.add(client().prepareIndex("articles", "comment", "4").setParent("2").setSource("message", "elephant captured"));
        requests.add(client().prepareIndex("articles", "comment", "5").setParent("2").setSource("message", "mice squashed by elephant x"));
        requests.add(client().prepareIndex("articles", "comment", "6").setParent("2").setSource("message", "elephant scared by mice x y"));
        indexRandom(true, requests);



        InnerHitsBuilder.InnerHit ih = new InnerHitsBuilder.InnerHit().setType("comment").setQuery(QueryBuilders.matchQuery("message", "fox"));


        SearchRequestBuilder prep = client().prepareSearch("articles");
        prep.setQuery(QueryBuilders.hasChildQuery("comment", QueryBuilders.matchQuery("message", "fox")));
        prep.addInnerHit("comment",ih);
        SearchResponse response = prep.get();

        assertNoFailures(response);
        assertHitCount(response, 1);
        assertSearchHit(response, 1, hasId("1"));

        Map<String, InternalSearchHits> f = new HashMap<>();


        Map<String, SearchHits> ihs = response.getHits().getAt(0).getInnerHits();

        assertThat(ihs.size(), equalTo(1));
        SearchHits innerHits = response.getHits().getAt(0).getInnerHits().get("comment");
        assertThat(innerHits.totalHits(), equalTo(2l));

        assertThat(innerHits.getAt(0).getId(), equalTo("1"));
        assertThat(innerHits.getAt(0).type(), equalTo("comment"));
        assertThat(innerHits.getAt(1).getId(), equalTo("2"));
        assertThat(innerHits.getAt(1).type(), equalTo("comment"));

        response = client().prepareSearch("articles")
                .setQuery(QueryBuilders.hasChildQuery("comment", QueryBuilders.matchQuery("message", "elephant")))
                .addInnerHit("comment", new InnerHitsBuilder.InnerHit().setType("comment").setQuery(QueryBuilders.matchQuery("message", "elephant")))
                .get();

        assertNoFailures(response);
        assertHitCount(response, 1);
        assertSearchHit(response, 1, hasId("2"));

        assertThat(response.getHits().getAt(0).getInnerHits().size(), equalTo(1));
        innerHits = response.getHits().getAt(0).getInnerHits().get("comment");
        assertThat(innerHits.totalHits(), equalTo(3l));

        assertThat(innerHits.getAt(0).getId(), equalTo("4"));
        assertThat(innerHits.getAt(0).type(), equalTo("comment"));
        assertThat(innerHits.getAt(1).getId(), equalTo("5"));
        assertThat(innerHits.getAt(1).type(), equalTo("comment"));
        assertThat(innerHits.getAt(2).getId(), equalTo("6"));
        assertThat(innerHits.getAt(2).type(), equalTo("comment"));
    }

    @Test
    public void testPathOrTypeMustBeDefined() {
        createIndex("articles");
        ensureGreen("articles");
        try {
            client().prepareSearch("articles")
                    .addInnerHit("comment", new InnerHitsBuilder.InnerHit())
                    .get();
        } catch (Exception e) {
            assertThat(e.getMessage(), containsString("Either [path] or [type] must be defined"));
            e.printStackTrace();
        }

    }

}