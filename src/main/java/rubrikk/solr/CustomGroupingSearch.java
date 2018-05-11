package rubrikk.solr;

import org.apache.lucene.document.Document;

import org.apache.lucene.search.Query;
import org.apache.solr.common.params.*;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.handler.component.ResponseBuilder;
import org.apache.solr.handler.component.SearchComponent;
import org.apache.solr.response.transform.DocTransformer;
import org.apache.solr.search.*;
import org.apache.solr.search.grouping.GroupingSpecification;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

public class CustomGroupingSearch extends SearchComponent  {

    private static final Logger Log = LoggerFactory.getLogger(CustomGroupingSearch.class);
    private Integer rows;
    private static final double[] SCORE_ARRAY = { 1, 0.89, 0.82, 0.78, 0.76, 0.75, 0.74, 0.73, 0.72, 0.71, 0.7, 0.69, 0.64, 0.55, 0.42, 0.28, 0.15, 0.1, 0.08, 0.06, 0.05, 0.04, 0.03, 0.02, 0.01 };

    @Override
    public void init(NamedList args)
    {

    }

    public void prepare(ResponseBuilder rb) throws IOException {
       SolrParams solrParams = rb.req.getParams();

       rows = solrParams.getInt(CommonParams.ROWS);
       Map<String,Object> mapParams = new HashMap<>();
       solrParams.toMap(mapParams);

        //TODO this might have to go into a custom queryparser
       mapParams.put(CommonParams.ROWS,40);
       mapParams.put(GroupParams.GROUP,true);
       mapParams.put(GroupParams.GROUP_FIELD,"domain");
       mapParams.put(GroupParams.GROUP_LIMIT,rows);
       //mapParams.put(CommonParams.RQ,"{!rerank reRankQuery=motoring__model_year:2014 reRankDocs=40 reRankWeight=3}");
       //mapParams.put(CommonParams.RQ,"{!rerank reRankQuery=motoring__model_year:2018 reRankDocs=40 reRankWeight=5}");
//       mapParams.put(CommonParams.RQ,"{!rerank reRankQuery=motoring__model_year:2017 reRankDocs=10 reRankWeight=4}");
       Log.info("Sorting: "+solrParams.get(CommonParams.SORT));

        rb.req.setParams(new MapSolrParams(mapParams.entrySet().stream()
                .collect(Collectors.toMap(x-> x.getKey(), x-> String.valueOf(x.getValue())))));
    }

    public void process(ResponseBuilder rb) throws IOException {


        SolrParams solrParams= rb.req.getParams();
        Log.info("Params in process: "+solrParams.toString());
        SolrIndexSearcher searcher = rb.req.getSearcher();

        Map<String,Object> paramMap = new HashMap<>();
        solrParams.toMap(paramMap);

        List<Query> filters =rb.getFilters();

        try {
            if (filters == null)
                filters = new ArrayList<>();
            QParser parser = QParser.getParser("motoring__model_year:2017",rb.req);

            filters.add(parser.getQuery());
        } catch (SyntaxError syntaxError) {
            syntaxError.printStackTrace();
        }

        QueryResult queryResult = new QueryResult();

        QueryCommand queryCommand = rb.createQueryCommand();

        GroupingSpecification groupSpecs= rb.getGroupingSpec();


        Grouping grouping = new Grouping(searcher,queryResult,queryCommand,false,0,groupSpecs.isMain() );

        grouping.setGroupSort(groupSpecs.getGroupSortSpec().getSort())
                .setWithinGroupSort(groupSpecs.getWithinGroupSortSpec().getSort())
                .setDefaultFormat(groupSpecs.getResponseFormat())
                .setLimitDefault(queryCommand.getLen())
                .setDocsPerGroupDefault(groupSpecs.getWithinGroupSortSpec().getCount())
                .setGroupOffsetDefault(groupSpecs.getWithinGroupSortSpec().getOffset())
                .setGetGroupedDocSet(groupSpecs.isTruncateGroups());


        if (groupSpecs.getFields() != null) {
            for (String field : groupSpecs.getFields()) {
                try {
                    grouping.addFieldCommand(field, rb.req);
                } catch (SyntaxError syntaxError) {
                    syntaxError.printStackTrace();
                }
            }
        }

        if (groupSpecs.getFunctions() != null) {
            for (String groupByStr : groupSpecs.getFunctions()) {
                try {
                    grouping.addFunctionCommand(groupByStr, rb.req);
                } catch (SyntaxError syntaxError) {
                    syntaxError.printStackTrace();
                }
            }
        }

        if (groupSpecs.getQueries() != null) {
            for (String groupByStr : groupSpecs.getQueries()) {
                try {
                    grouping.addQueryCommand(groupByStr, rb.req);
                } catch (SyntaxError syntaxError) {
                    syntaxError.printStackTrace();
                }
            }
        }

        if( rb.isNeedDocList() || rb.isDebug() ){
            // we need a single list of the returned docs
            queryCommand.setFlags(SolrIndexSearcher.GET_DOCLIST);
        }

        grouping.execute();

//        SimpleOrderedMap<RubrikkGrouping.Root> dataObject = (SimpleOrderedMap) queryResult.groupedResults;
//
//        if(dataObject !=null)
//        {
//            for(Map.Entry<String,RubrikkGrouping.Root> ob: dataObject.asShallowMap().entrySet())
//            {
//                 Log.info("Group class "+ob.getKey());
//            }

//        }

        SimpleOrderedMap grouped = (SimpleOrderedMap) queryResult.groupedResults;

        Map<String,Object> dataMap = grouped.asShallowMap();

        for(Map.Entry<String,Object> entry: dataMap.entrySet())
        {
            Log.info("Loop 1: ");
           // Log.info("Key: "+entry.getKey()+" value: "+entry.getValue());
            Map<String,Object> domains = (HashMap)entry.getValue();



            for(Map.Entry<String,Object> obj:domains.entrySet())
            {
                Log.info("Loop 2: ");
                //Log.info("Key: "+obj.getKey()+" value: "+obj.getValue());
                try
                {
                    ArrayList<Object> doclist = (ArrayList)obj.getValue();

                    for(Object doc:doclist)
                    {
                        Log.info("Loop 3: ");
                        SimpleOrderedMap<Object> objMap = (SimpleOrderedMap)doc;


                        //Log.info("Key: "+objMap.entrySet().getKey()+" value: "+objMap.getValue());
                        //Map<String,Object> docList = (HashMap)objMap.getValue();

                        for (Map.Entry<String,Object> docSlices:objMap.asShallowMap().entrySet())
                        {
                            Log.info("Loop 4: ");
                            //Log.info("Key: "+docSlices.getKey()+" value: "+docSlices.getValue());
                            if(docSlices.getValue() instanceof DocSlice )
                            {
                                DocSlice docSlice = (DocSlice) docSlices.getValue();
                                DocList docs = docSlice.subset(0,rows);
                                DocIterator iterator = docs.iterator();
                                for(int i =0; i<docs.size(); i++)
                                {
                                    int docsetid = iterator.nextDoc();
                                    //float sc = iterator.score();
                                    Log.info("Doc Id: "+docsetid);
                                    //Log.info("Doc score: "+iterator.score());
                                   // Document d = searcher.doc(iterator.nextDoc());
                                }

                            }
                        }
                    }
                }
                catch (ClassCastException ex)
                {
                    Log.error("Loop 2 error "+ex.getMessage());
                }
            }
        }

        QueryResult rslt = searcher.search(queryResult,rb.createQueryCommand());

//
//        Log.info("get field flags: "+rb.getFieldFlags());
//        Log.info("filters: "+filters.toString());
//
//
//
//        DocList featureDocList = searcher.
//                getDocList(rb.getQuery(),filters,rb.getSortSpec().getSort(),0,40,rb.getFieldFlags());

//        Set<String> fieldsSet = new HashSet<>();
//        fieldsSet.add("score");
//        fieldsSet.add("ID");
//        fieldsSet.add("Campaign_id");
//
//        List<String> campingIdScorePair = Arrays.asList(solrParams.getParams("Campaign"));
//        Log.info("extra param size: "+campingIdScorePair.size());
//        Map<String,String> CampaignIdBoost = new HashMap<>();
//
//        campingIdScorePair.stream().forEach(x->{
//            String[] pair = x.split(":");
//            CampaignIdBoost.put(pair[0],pair[1]);
//        });
//
//        CampaignIdBoost.entrySet().stream().forEach(
//                c-> Log.info("Campaign params key: "+c.getKey()+" and value "+c.getValue()));
//
//        DocList initialDocList = rb.getResults().docList;
//
//        DocIterator iterator = initialDocList.iterator();
//        Map<String, String> scoreMap = new HashMap<>();
//
//        Log.info("doc size: "+initialDocList.size());
//
//        //for distribuited search only
//        //SolrDocumentList docList = rb.getResponseDocs();
//
//        for(int i = 0;i<initialDocList.size();i++)
//        {
//            try {
//                int docId = iterator.nextDoc();
//                SolrDocumentFetcher documentFetcher =  searcher.getDocFetcher();
//                SolrDocument doc = new SolrDocument();
//
//                documentFetcher.decorateDocValueFields(doc, docId,fieldsSet);
//                Document d = searcher.doc(docId,fieldsSet);
//
//                Log.info("doc campaign id "+doc.get("Campaign_id"));
//                if (CampaignIdBoost.containsKey(String.valueOf(doc.get("Campaign_id")) )){
//                   Double newScore = Double.valueOf(1)
//                           * Double.valueOf(1.3);
//                    doc.addField("newscore",newScore);
//
//                    Log.info("Doc id: "+d.getField("ID").name());
//                    scoreMap.put(String.valueOf("new score"),String.valueOf(newScore));
//                    Log.info("new Score: "+newScore);
//                }
//
//            }catch (SolrException ex)
//            {new score
//                Log.error("Exception: "+ex.getMessage());
//            }
//        }
//
//
        rb.rsp.add("group: ",grouped);
        }

    public String getDescription() {
        return "CustomGroupingSearch";
    }


    public NamedList<Object> getStatistics(){
        return null;
   }

   private class GroupObject {

   }

}
