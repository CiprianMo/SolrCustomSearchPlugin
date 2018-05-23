package rubrikk.solr;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Sort;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.*;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.handler.component.QueryComponent;
import org.apache.solr.handler.component.ResponseBuilder;
import org.apache.solr.handler.component.SearchComponent;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.BasicResultContext;
import org.apache.solr.response.transform.DocTransformer;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.search.*;
import org.apache.solr.search.grouping.GroupingSpecification;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

public class CustomGroupingSearch extends SearchComponent  {

    private static final Logger Log = LoggerFactory.getLogger(CustomGroupingSearch.class);
    private static final String FIELD_TO_APPEND = "grouporderscore";
    private static CampaingScoreTransformFactory transformFactory;
    private Integer rows;
    private Set<String> fls;
    private static final float[] SCORE_ARRAY = { 1, 0.89f, 0.82f, 0.78f, 0.76f, 0.75f, 0.74f, 0.73f, 0.72f, 0.71f, 0.7f, 0.69f, 0.64f, 0.55f, 0.42f, 0.28f, 0.15f, 0.1f, 0.08f, 0.06f, 0.05f, 0.04f, 0.03f, 0.02f, 0.01f };


    public void prepare(ResponseBuilder rb) throws IOException {

        //TODO validate fields here somewhere
        SolrParams solrParams = rb.req.getParams();
        rows = solrParams.getInt(CommonParams.ROWS);

        Map<String,Object> mapParams = new HashMap<>();
        solrParams.toMap(mapParams);

        //TODO this might have to go into a custom queryparser
        mapParams.put(CommonParams.ROWS,100);
        //mapParams.put(GroupParams.GROUP,true);
        //mapParams.put(GroupParams.GROUP_FIELD,"domain");
        //mapParams.put(GroupParams.GROUP_LIMIT,rows);

        Log.info("Sorting: "+solrParams.get(CommonParams.SORT));

        rb.req.setParams(new MapSolrParams(mapParams.entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, x-> String.valueOf(x.getValue())))));

        transformFactory = new CampaingScoreTransformFactory();
        ObjectMapper objectMapper = new ObjectMapper();

//TODO check is this param is set
        NamedList<Object>campaigns = new NamedList<>(objectMapper.readValue(solrParams.get("Campaign"),HashMap.class));
        campaigns.add("0",1.0f);

        Log.info("Campaigsn list: "+campaigns);

        transformFactory.init(campaigns);

        Collection<SchemaField> fields = rb.req.getSchema().getFields().values();
        fls = fields.stream().map(SchemaField::getName).collect(Collectors.toSet());
    }

    public void process(ResponseBuilder rb) throws IOException {

        DocTransformer docTransformer = transformFactory.create(FIELD_TO_APPEND,rb.req.getParams(),rb.req);

        SolrParams solrParams = rb.req.getParams();

        Map<String,Object> mapParams = new HashMap<>();
        solrParams.toMap(mapParams);

        //TODO this might have to go into a custom queryparser
//        mapParams.put(CommonParams.ROWS,100);
//        mapParams.replace(GroupParams.GROUP,true);
//        mapParams.put(GroupParams.GROUP_FIELD,"domain");
//        mapParams.put(GroupParams.GROUP_LIMIT,rows);

        Log.info("Sorting: "+solrParams.get(CommonParams.SORT));

        rb.req.setParams(new MapSolrParams(mapParams.entrySet().stream()
                .collect(Collectors.toMap(x-> x.getKey(), x-> String.valueOf(x.getValue())))));

        SolrIndexSearcher searcher = rb.req.getSearcher();

        List<Query> filters =rb.getFilters();
        List<Query> normalFilters = new ArrayList<>();

        QueryResult queryResultFeatured = new QueryResult();
        
        QueryResult queryResultNormal = new QueryResult();

       QueryCommand queryCommandFeatured = rb.createQueryCommand();

       QueryCommand queryCommandNormal = rb.createQueryCommand();

       for(Query query:filters){
           if(!query.toString().contains("Campaign_id")){
               normalFilters.add(query);
           }
       }

       queryCommandNormal.setFilterList(normalFilters);


       SortSpec sortSpec = rb.getSortSpec();

       if (sortSpec == null){
           Log.error("SortSpecs is null");
       }
       SortSpec groupSortSpec = searcher.weightSortSpec(sortSpec, Sort.RELEVANCE);



       GroupingSpecification groupSpecs = new GroupingSpecification();

       prepareGrouping(rb, groupSpecs);

        if(groupSpecs !=null)
            Log.info("Group specs not null");
        if(groupSpecs == null)
            Log.info("groups secs null");

        for(Query q:queryCommandFeatured.getFilterList())
           Log.info("Filter query "+q.toString());

        for(Query qs:queryCommandNormal.getFilterList())
            Log.info("Filter query for normal"+qs.toString());
        
        

        PerformGroupSearch(rb, searcher, queryResultFeatured, queryCommandFeatured, groupSpecs);
        
        PerformGroupSearch(rb, searcher, queryResultNormal, queryCommandNormal, groupSpecs);
        

//        SimpleOrderedMap<RubrikkGrouping.Root> dataObject = (SimpleOrderedMap) queryResultFeatured.groupedResults;
//
//        if(dataObject !=null)
//        {
//            for(Map.Entry<String,RubrikkGrouping.Root> ob: dataObject.asShallowMap().entrySet())
//            {
//                 Log.info("Group class "+ob.getKey());
//            }

//        }

        SolrDocumentList solrDocumentListFeatured = getSolrDocuments((CampaingScoreTransformFactory.CampaingsScoreTransformer) docTransformer, searcher, queryResultFeatured,true);

        solrDocumentListFeatured.setNumFound(solrDocumentListFeatured.size());
        //TODO don't boost on camapaign ids at all here
        SolrDocumentList solrDocumentListNormal = getSolrDocuments((CampaingScoreTransformFactory.CampaingsScoreTransformer) docTransformer, searcher, queryResultNormal,false);

        solrDocumentListNormal.setNumFound(solrDocumentListNormal.size());

        Collections.sort(solrDocumentListFeatured,Collections.reverseOrder((o1,o2)-> Float.compare((Float)o1.getFieldValue(FIELD_TO_APPEND),(Float)o2.getFieldValue(FIELD_TO_APPEND))));

        Collections.sort(solrDocumentListNormal,Collections.reverseOrder((o1,o2)-> Float.compare((Float)o1.getFieldValue(FIELD_TO_APPEND),(Float)o2.getFieldValue(FIELD_TO_APPEND))));

        Log.info("solrDocument size: "+solrDocumentListFeatured.size());

        NamedList<Object> responseData = new NamedList<>();
        responseData.add("featured ads",solrDocumentListFeatured);
        responseData.add("normal ads", solrDocumentListNormal);

//        Map<String,Object> mapParam = new HashMap<>();
//        solrParams.toMap(mapParam);
//        if(mapParam.containsKey(GroupParams.GROUP))
//            mapParam.replace(GroupParams.GROUP,false);
//
//        rb.req.setParams(new MapSolrParams(mapParam.entrySet().stream()
//                .collect(Collectors.toMap(x-> x.getKey(), x-> String.valueOf(x.getValue())))));

        //QueryResult rslt = searcher.search(queryResultFeatured,rb.createQueryCommand());
        //((SimpleOrderedMap) queryResultFeatured.groupedResults).clear();
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
        Log.info("Response getValues "+rb.rsp.getValues().size());

        NamedList<Object> response = rb.rsp.getValues();
        Log.info("Response size: "+response.size());
                for (int i = 0; i < response.size();++i)
        {
            if(response.getVal(i) instanceof BasicResultContext){
                BasicResultContext resultCont = (BasicResultContext)response.getVal(i);
                response.remove(i);
            //Log.info("Response docList size: "+resultCont.getDocList().size());
            }

//            Log.info("Response name at index "+i+" "+response.getName(i));
//            Log.info("Response ob at index "+i+" "+response.getVal(i));
        }

        rb.rsp.addResponse(responseData);

        }


    private void prepareGrouping(ResponseBuilder rb,GroupingSpecification groupingSpec) throws IOException{
        SolrQueryRequest req = rb.req;

        SolrIndexSearcher searcher = rb.req.getSearcher();

        final SortSpec sortSpec = rb.getSortSpec();

        final SortSpec groupSortSpec = searcher.weightSortSpec(sortSpec, Sort.RELEVANCE);

        SortSpec withinGroupSortStr = new SortSpec(
                groupSortSpec.getSort(),
                groupSortSpec.getSchemaFields(),
                groupSortSpec.getCount(),
                groupSortSpec.getOffset());
        withinGroupSortStr.setOffset(0);
        withinGroupSortStr.setCount(rows);

        groupingSpec.setWithinGroupSortSpec(withinGroupSortStr);
        groupingSpec.setGroupSortSpec(groupSortSpec);
        String formatStr = "grouped";
        Grouping.Format responseFormat;
        try {
            responseFormat = Grouping.Format.valueOf(formatStr);
        } catch (IllegalArgumentException e) {
            throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, String.format(Locale.ROOT, "Illegal %s parameter", GroupParams.GROUP_FORMAT));
        }
        groupingSpec.setResponseFormat(responseFormat);
        groupingSpec.setFields(new String[]{"domain"});
        groupingSpec.setNeedScore((rb.getFieldFlags() & SolrIndexSearcher.GET_SCORES) != 0);
    }

    private SolrDocumentList getSolrDocuments(CampaingScoreTransformFactory.CampaingsScoreTransformer docTransformer, SolrIndexSearcher searcher, QueryResult queryResultFeatured, boolean boostCampaigns) throws IOException {
        SimpleOrderedMap grouped = (SimpleOrderedMap) queryResultFeatured.groupedResults;

        Map<String,Object> dataMap = grouped.asShallowMap();

        SolrDocumentFetcher docFetcher =  searcher.getDocFetcher();

        SolrDocumentList solrDocumentListFeatured = new SolrDocumentList();

        for(Map.Entry<String,Object> entry: dataMap.entrySet())
        {
            //Log.info("Loop 1: ");
           // Log.info("Key: "+entry.getKey()+" value: "+entry.getValue());
            Map<String,Object> domains = (HashMap)entry.getValue();



            for(Map.Entry<String,Object> obj:domains.entrySet())
            {

                //Log.info("Key: "+obj.getKey()+" value: "+obj.getValue());
                try
                {
                    ArrayList<Object> doclist = (ArrayList)obj.getValue();


                    for(Object doc:doclist)
                    {
                        //Log.info("Loop 3: ");
                        SimpleOrderedMap<Object> objMap = (SimpleOrderedMap)doc;


                        //Log.info("Key: "+objMap.entrySet().getKey()+" value: "+objMap.getValue());
                        //Map<String,Object> docList = (HashMap)objMap.getValue();

                        for (Map.Entry<String,Object> docSlices:objMap.asShallowMap().entrySet())
                        {
                            //Log.info("Loop 4: ");
                            //Log.info("Key: "+docSlices.getKey()+" value: "+docSlices.getValue());
                            if(docSlices.getValue() instanceof DocSlice)
                            {
                                int index =0;
                                DocSlice docSlice = (DocSlice) docSlices.getValue();
                                //TODO change hardcoded len
                                DocList docs = docSlice.subset(0,rows);
                                DocIterator iterator = docs.iterator();

                                Log.info("docs.size "+docs.size());
                                for(int i =0; i<docs.size(); i++)
                                {
                                    index = Math.min(index,SCORE_ARRAY.length-1);
                                    Log.info("Index "+index);
                                    int docsetid = iterator.nextDoc();

                                    SolrDocument docBase = new SolrDocument();
                                    docFetcher.decorateDocValueFields(docBase,docsetid,fls);
                                    Document d = searcher.doc(i,fls);
                                    docBase.addField("score",iterator.score());

                                    for(IndexableField field:d.getFields())
                                    {

                                       // Log.info("field name "+field.name()+" field value "+field.stringValue());

                                        docBase.addField(field.name(),field.stringValue());
                                    }
                                    //Log.info("Campaign field "+docBase.getFieldValue("Campaign_id"));

                                    docTransformer
                                            .transform(docBase,iterator.score(),SCORE_ARRAY[index++],boostCampaigns);

                                    solrDocumentListFeatured.add(docBase);
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

        return solrDocumentListFeatured;
    }

    private void PerformGroupSearch(ResponseBuilder rb, SolrIndexSearcher searcher, QueryResult queryResult, QueryCommand queryCommand, GroupingSpecification groupSpecs) throws IOException {
        Grouping grouping = new Grouping(searcher,queryResult,queryCommand,false,0,false );

        grouping.setGroupSort(rb.getSortSpec().getSort())
                .setWithinGroupSort(groupSpecs.getWithinGroupSortSpec().getSort())
                .setDefaultFormat(groupSpecs.getResponseFormat())
                .setLimitDefault(queryCommand.getLen())
                .setDocsPerGroupDefault(groupSpecs.getWithinGroupSortSpec().getCount())
                .setGroupOffsetDefault(0)
                .setGetGroupedDocSet(groupSpecs.isTruncateGroups());

           if (groupSpecs.getFields() != null) {
            for (String field : groupSpecs.getFields()) {
                try {
                    grouping.addFieldCommand("domain", rb.req);
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

        //TODO check if this fist of second pass score
        queryCommand.setFlags(SolrIndexSearcher.GET_SCORES);
        grouping.execute();
    }

    public String getDescription() {
        return "CustomGroupingSearch";
    }


    public NamedList<Object> getStatistics(){
        return null;
   }

    public class Campaign {
        public String id;
        public Object boost;
    }

}

