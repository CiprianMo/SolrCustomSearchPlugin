package rubrikk.solr;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Sort;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.MapSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.handler.component.ResponseBuilder;
import org.apache.solr.handler.component.SearchComponent;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.search.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

public class Deduplication extends SearchComponent {

    private static final Logger Log = LoggerFactory.getLogger(Deduplication.class);
    private static final String DUPLICATIONFIELD = String.valueOf("Duplicate_grp_id");

    private static Integer rows;

    @Override
    public void prepare(ResponseBuilder rb) throws IOException {

            SolrParams params = rb.req.getParams();

        rows = Integer.valueOf(params.get(CommonParams.ROWS));
    }

    @Override
    public void process(ResponseBuilder rb) throws IOException {
        //TODO take this field either from solrconfig or from query param

        SchemaField duplicateIdField = rb.req.getCore().getLatestSchema().getField(DUPLICATIONFIELD);

        SchemaField identifyField = rb.req.getCore().getLatestSchema().getUniqueKeyField();

        SolrIndexSearcher searcher = rb.req.getSearcher();

        Set<String> fieldSet = new HashSet<>();

        fieldSet.add(duplicateIdField.getName());
        fieldSet.add(identifyField.getName());

        DocList initialDocList = rb.getResults().docList;

        DocIterator iterator = initialDocList.iterator();

        Set<String> uniqueIds = new HashSet<>();
        Set<String> duplicateIds = new HashSet<>();

        NamedList<String> dublications = new NamedList<>();
        //TODO verify for null
        for(int i = 0; i<initialDocList.size(); i++){
            try {
                int docId = iterator.nextDoc();

                Log.info("initialdocid: "+docId);

                SolrDocumentFetcher docFetcher =  searcher.getDocFetcher();

                SolrDocument docBase = new SolrDocument();
                docFetcher.decorateDocValueFields(docBase,docId,fieldSet);
                Map<String,Object> obj = docBase.getFieldValueMap();
                String value = String.valueOf(obj.get(DUPLICATIONFIELD));

                if(uniqueIds.contains(value)){
                   duplicateIds.add(value);
                   dublications.add(duplicateIdField.getName(),value);
                }
                else {
                    uniqueIds.add(value);
                }
            }catch (IOException ex){
                Log.error("Error: "+ex.getMessage());
            }
        }

        SolrParams params = rb.req.getParams();

        NamedList<Object> paramsNamedList = params.toNamedList();
        paramsNamedList.add(CommonParams.FQ,"-itemindex:(127178378 111193779)");

        Map<String,String> paramMap = paramsNamedList.asShallowMap().entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey,e-> String.valueOf(e.getValue())));

        Log.info("Map params: "+paramMap);

        rb.req.setParams(new MapSolrParams(paramMap));

        List<Query> filters = rb.getFilters();

        try {
            QParser parser = QParser.getParser("-itemindex:(127178378 111193779)",rb.req);
            if(filters == null)
                filters = new ArrayList<>();
            filters.add(parser.getQuery());
        } catch (SyntaxError syntaxError) {
            syntaxError.printStackTrace();
        }

        //rb.req.getSearcher().search(rb.getQuery(), Integer.valueOf(rows));

        Log.info("NamedList params: "+paramsNamedList);

        //NamedList<Object> response = rb.rsp.getValues();

        Sort srt =rb.getSortSpec().getSort();

        Log.info("Modifued filters "+rb.getFilters());


        //FilterQuery fq = new FilterQuery()

        Log.info("Modified query"+rb.getQuery());

        DocList docSet = searcher.getDocList(rb.getQuery(),filters,srt,0,rows,rb.getFieldFlags());



        DocIterator docsetIterator = docSet.iterator();
        for(int i =0; i<docSet.size(); i++)
        {
            int docsetid = docsetIterator.nextDoc();
            Log.info("DocsetId: "+docsetid);
        }

//        for (int i = 0; i < response.size();i++)
//        {
//            if(response.getVal(i) instanceof BasicResultContext){
//                BasicResultContext resultCont = (BasicResultContext)response.getVal(i);
//                //response.remove(i);
//            Log.info("Response docList size: "+resultCont.getDocList().size());
//            }
//
//            Log.info("Response name at index "+i+" "+response.getName(i));
//            Log.info("Response ob at index "+i+" "+response.getVal(i));
//        }
        //rb.rsp.setAllValues(response);
        //Log.info("reasponse size: "+response.size());
        //rb.rsp.setAllValues();
        rb.rsp.add("duplications: ",dublications);
        rb.rsp.add("unique: ",uniqueIds);
        rb.rsp.add("new response",docSet);
    }

    @Override
    public String getDescription() {
        return "Duplications";
    }

    @Override
    public Map<String, Object> getMetricsSnapshot() {
        return null;
    }

    @Override
    public void registerMetricName(String name) {

    }
}

