package rubrikk.solr;

import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.search.Query;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.PluginInfo;
import org.apache.solr.core.SolrCore;
import org.apache.solr.handler.component.ResponseBuilder;
import org.apache.solr.handler.component.SearchComponent;
import org.apache.solr.handler.component.ShardRequest;
import org.apache.solr.handler.component.ShardResponse;
import org.apache.solr.search.DocIterator;
import org.apache.solr.search.DocList;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.util.plugin.PluginInfoInitialized;
import org.apache.solr.util.plugin.SolrCoreAware;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Iterator;

public class CustomGroupingSearch extends SearchComponent implements PluginInfoInitialized, SolrCoreAware {

    private static final Logger Log = LoggerFactory.getLogger(CustomGroupingSearch.class);

    public void init(PluginInfo pluginInfo) {

    }

    public void prepare(ResponseBuilder rb) throws IOException {
        Log.info("Prepare method");
        String para=rb.req.getParamString();

        Log.info("Req Param: "+para );

        String resp = (String) rb.rsp.getResponse();

        Log.info("Resp: "+resp);

    }

     public void inform(SolrCore solrCore) {
        Log.info("Core schema: "+solrCore.getSchemaResource());
    }

    public void process(ResponseBuilder rb) throws IOException {
        Log.info("Process method");
        SolrIndexSearcher searcher = rb.req.getSearcher();
        LeafReader reader = searcher.getSlowAtomicReader();

         int docCounter = 0;
        DocList docList = rb.getResults().docList;
        for (DocIterator it  = docList.iterator(); it.hasNext();) {
            Log.info("docid = "+it.nextDoc());
           docCounter++;
        }

        Log.info("docList size "+ docList.size());




        Log.info("Doc counter= "+docCounter);


        Log.info("response: "+rb.rsp.getReturnFields());

       Query q = rb.getQuery();

       if(q==null)
       {
           Log.info("query null");
       }
       else
            Log.info("Query: "+q);

       Log.info("Searcher num doc: "+ rb.req.getSearcher().numDocs());
    }

    public void handleResponses(ResponseBuilder rb, ShardRequest sreq)
    {
        rb.outgoing.size();

        Log.info("In the response ahndler");

        Log.info("response size: "+ sreq.responses.size());

        Iterator var5 = sreq.responses.iterator();

        while(var5.hasNext()) {
            ShardResponse srsp = (ShardResponse)var5.next();
            NamedList response = srsp.getSolrResponse().getResponse();
            String ex = response.toString();

            Log.info("Response to string: "+ex);
        }

        Log.info("Response: "+rb.rsp.getResponse());
        Log.info("Shard: "+sreq.toString());
    }

    public void finishStage(ResponseBuilder rb)
    {
        Log.info("Inside the finishStage");
    }

     public int distributedProcess(ResponseBuilder rb) throws IOException {
        Log.info("insidddddde the distr");
        return super.distributedProcess(rb);
     }

    public String getDescription() {
        return "CustomGroupingSearch";
    }




}
