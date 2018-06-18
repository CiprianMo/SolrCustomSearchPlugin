package rubrikk.solr;

import org.apache.solr.common.SolrDocument;
import org.junit.Assert;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class CampaingSocreTransformFactoryTest {



    @BeforeAll
    public static void init(){

    }

    @Test
    void Calculates_score_correctly(){
        CampaignScoreTransformFactory transformer = new CampaignScoreTransformFactory();
        CampaignScoreTransformFactory.CampaignsScoreTransformer cc =transformer.create("orderscore",null,null);

        SolrDocument solrDoc = new SolrDocument();
        solrDoc.addField("Quality_boost",-2.0);

        cc.transform(solrDoc,-4f,0.89f,false);

        Assert.assertEquals(8f*0.89f,solrDoc.getFieldValue("orderscore"));
    }
}
