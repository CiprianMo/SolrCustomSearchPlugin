package rubrikk.solr;

import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.GroupParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.handler.component.ResponseBuilder;
import org.apache.solr.request.SolrQueryRequest;
import org.junit.Assert;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

class CustomGroupingSearchTest {

    private static CustomGroupingSearch customGroupingSearch ;
    private static SolrDocumentList list;

    @BeforeAll
    public static void init() {

        customGroupingSearch = new CustomGroupingSearch();
        list = new SolrDocumentList();

        SolrDocument document = new SolrDocument();
        document.setField("duplicatehash","c9c8a31d-69ad-19a7-d2dd-e217b876d4b0");
        document.setField("quality",1.0f);
        list.add(document);

        SolrDocument document1 = new SolrDocument();
        document1.setField("duplicatehash","c9c8a31d-69ad-19a7-d2dd-e217b876d4b0");
        document1.setField("quality",2.1f);
        list.add(document1);

        SolrDocument document2 = new SolrDocument();
        document2.setField("duplicatehash","c5c8a31d-69ad-19a7-d2dd-e217b876d4b5");
        document2.setField("quality",2.0f);
        list.add(document2);

        SolrDocument document3 = new SolrDocument();
        document3.setField("duplicatehash","c3c8a31d-69ad-19a7-d2dd-e217b87644b0");
        document3.setField("quality",2.0f);
        list.add(document3);

        SolrDocument document4 = new SolrDocument();
        document4.setField("duplicatehash","c1c8a31d-69ad-19a7-d2dd-e217b376d4b0");
        document4.setField("quality",2.0f);
        list.add(document4);
    }

    @AfterEach
    void tearDown() {

    }

    @Test
    void addFeaturedParams_Adds_Filter_Campaigns() {
        List<Integer> campaignIds = new ArrayList<>();
        campaignIds.add(1);
        campaignIds.add(2);

        NamedList params = new NamedList();

        String filters = (String) customGroupingSearch.addFeaturedParams(params,campaignIds).get(CommonParams.FQ);

        Assert.assertEquals(filters,"Campaign_id:[1 TO 1] OR Campaign_id:[2 TO 2]");

    }

    @Test
    void prepare_Throws_Bad_Request(){

    }

    @Test
    void parseNormalParams_Adds_Grouping_Params() {
        NamedList solrParams = new NamedList();

        solrParams.add(CommonParams.FL,"*");
        customGroupingSearch.parseNormalParams(solrParams);

        Assert.assertNotNull(solrParams.get(GroupParams.GROUP));
        Assert.assertNotNull(solrParams.get(GroupParams.GROUP_FIELD));
        Assert.assertNotNull(solrParams.get(GroupParams.GROUP_FORMAT));
        Assert.assertNotNull(solrParams.get(GroupParams.GROUP_LIMIT));

    }

    @Test
    void deDuplicateBasedOnQuality_Filters_The_Duplicated_Docs() {

        SolrDocumentList distinctList = customGroupingSearch.deDuplicateBasedOnQuality(list);

         Assert.assertEquals(4,distinctList.size());
         Assert.assertTrue(distinctList.stream().filter(o->(Float)o.getFieldValue("quality")<2.0).count() ==0 );
    }
}