package rubrikk.solr;

import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.transform.DocTransformer;
import org.apache.solr.response.transform.TransformerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;


public class CampaingScoreTransformFactory extends TransformerFactory {
    private static final Logger Log = LoggerFactory.getLogger(CampaingScoreTransformFactory.class);

    Map<String,Float> campaigns;

    @Override
    public void init(NamedList args){

        Log.info("Args "+args.toString());
        campaigns=args.asShallowMap();
    }

    @Override
    public CampaingsScoreTransformer create(String field, SolrParams params, SolrQueryRequest req) {
        return new CampaingsScoreTransformer(field,campaigns);
    }

    class CampaingsScoreTransformer extends DocTransformer{

        final String name;
        final Map<String,Float> campaigns;
        private float value;


        public CampaingsScoreTransformer(String name,Map<String,Float> campaigns) {
            this.name = name;
            this.campaigns = campaigns;
        }

        @Override
        public String getName() {
            return name;
        }

         public void transform(SolrDocument doc,float score,float positionScore){

             String docCampaignId = String.valueOf(doc.getFieldValue("Campaign_id"));

             Float docQualityBoost = ((Double)doc.getFieldValue("Quality_boost")).floatValue();

             value = docQualityBoost;

             if(!Float.valueOf(score).isNaN())
                value *= score;

             if(campaigns.containsKey(docCampaignId))
                 value *= campaigns.get(docCampaignId);

             value*=positionScore;

             doc.setField(name,value);
         }

        @Override
        public void transform(SolrDocument doc, int docid) throws IOException {

            doc.setField(name,value);
        }
    }
}
