package rubrikk.solr;

import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.transform.DocTransformer;
import org.apache.solr.response.transform.TransformerFactory;

import java.util.HashMap;
import java.util.Map;


public class CampaignScoreTransformFactory extends TransformerFactory
{

    private Map campaigns = new HashMap<>();

    @Override
    public void init(NamedList args)
    {
        campaigns = args.asShallowMap();
    }

    @Override
    public CampaignsScoreTransformer create(String field, SolrParams params, SolrQueryRequest req)
    {
        return new CampaignsScoreTransformer(field,campaigns);
    }

    class CampaignsScoreTransformer extends DocTransformer
    {
        final String name;
        final Map<String,Float> campaigns;
        private float value;


        public CampaignsScoreTransformer(String name, Map<String,Float> campaigns)
        {
            this.name = name;
            this.campaigns = campaigns;
        }

        @Override
        public String getName() {
            return name;
        }


        public void transform(SolrDocument doc,float score,float positionScore,boolean boostCampaigns)
        {
            String docCampaignId = String.valueOf(doc.getFieldValue("Campaign_id"));

            value = ((Double)doc.getFieldValue("Quality_boost")).floatValue();

            if(!Float.valueOf(score).isNaN())
               value *= score;

            if(boostCampaigns && campaigns.containsKey(docCampaignId))

                 //TODO this looks very weird
                value *= Float.valueOf(String.valueOf(campaigns.get(docCampaignId)));

            value *= positionScore;

            doc.setField(name,value);
        }

        protected void appendField(SolrDocument doc, String name, String value)
        {
            doc.setField(name,value);
        }

        @Override
        public void transform(SolrDocument doc, int docid)
        {
            doc.setField(name,value);
        }
    }
}
