package rubrikk.solr;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexableField;

import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.handler.component.ResponseBuilder;
import org.apache.solr.handler.component.SearchComponent;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.search.DocIterator;
import org.apache.solr.search.DocList;
import org.apache.solr.search.SolrIndexSearcher;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class CustomGroupingSearch extends SearchComponent  {

    private static final Logger Log = LoggerFactory.getLogger(CustomGroupingSearch.class);

    volatile long numRequests;
    volatile long numeErrors;
    volatile long totalRequestTime;
    volatile String lastNewSearcher;
    volatile String lastOptimizedEvent;
    volatile String defaultField;

    private List<String> words;

    @Override
    public void init(NamedList args)
    {
        super.init(args);
        defaultField = (String)args.get("field");

        if(defaultField == null)
            throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,"Need To Specify the default for analysis");

        words = ((NamedList)args.get("words")).getAll("word");

    }

    public void prepare(ResponseBuilder responseBuilder) throws IOException {

    }

    public void process(ResponseBuilder rb) throws IOException {
        numRequests++;
        SolrParams solrParams = rb.req.getParams();
        long lstartTime = System.currentTimeMillis();
        SolrIndexSearcher searcher = rb.req.getSearcher();
        NamedList respones = new SimpleOrderedMap();

        String queryFields = solrParams.get("field");

        String field = null;
        if(defaultField!=null){
            field = defaultField;
        }

        if(queryFields!=null)
            field = queryFields;

        if(field==null) {
            Log.error("Fields aren't defined ");
            return;
        }
        DocList docs = rb.getResults().docList;
        if(docs == null || docs.size()==0)
            Log.info("No results");
        Log.info("Doing this many results:\t"+docs.size());

        Set<String> fieldSet = new HashSet<>();

        SchemaField keyFields = rb.req.getCore().getLatestSchema().getUniqueKeyField();

        if(null != keyFields){
            fieldSet.add(keyFields.getName());
        }

        fieldSet.add(field);

        DocIterator iterator = docs.iterator();
        for (int i = 0; i< docs.size(); i++){
            try {
                int docId = iterator.nextDoc();
                HashMap<String, Double> counts = new HashMap<String,Double>();

                Document doc = searcher.doc(docId,fieldSet);
                if(doc == null){
                    Log.error("Doc not found, id: "+docId);
                }
                IndexableField[] multifield = doc.getFields(field);

                for(IndexableField singleField:multifield){
                    for (String string:singleField.stringValue().split(" ")){
                        if(words.contains(string)){
                            Double oldcount = counts.containsKey(string)?counts.get(string):0.0;
                            counts.put(string, oldcount+1);
                        }
                    }
                }
                IndexableField ident = doc.getField(keyFields.getName());
                if(ident == null)
                {
                    Log.error("ident field is null ");
                }
                String id = doc.getField(keyFields.getName()).stringValue();

                NamedList<Double> docresults = new NamedList<>();
                for(String word:words){
                    docresults.add(word,counts.get(word));
                }

                respones.add(id,docresults);
            }
            catch (IOException ex){
                Log.error("Error: "+ex.getMessage());
            }
        }
        rb.rsp.add("demoSearchComponent: ",respones);
        totalRequestTime+=System.currentTimeMillis()-lstartTime;
            }

    public String getDescription() {
        return "CustomGroupingSearch";
    }


    public NamedList<Object> getStatistics(){
        NamedList all = new SimpleOrderedMap<Object>();
        all.add("requests",""+numRequests );
        all.add("errors",""+numeErrors);
        all.add("totalTime(ms)",""+totalRequestTime);
        return all;
    }

}
