package rubrikk.solr;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.search.Query;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.params.*;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.handler.component.ResponseBuilder;
import org.apache.solr.handler.component.SearchComponent;
import org.apache.solr.response.transform.DocTransformer;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.search.*;
import org.apache.solr.search.grouping.GroupingSpecification;
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
    private static final Map<Integer,Double> Campaigns = new HashMap<>();
    @Override
    public void init(NamedList args)
    {
        Campaigns.put(0,1.3);
        Campaigns.put(1,1.6);
        Campaigns.put(2,1.8);
    }

    public void prepare(ResponseBuilder rb) throws IOException {

        //TODO validate fields here somewhere
        SolrParams solrParams = rb.req.getParams();

        NamedList<Float> campaigns = new NamedList<>();
        campaigns.add("0",1.3f);
        campaigns.add("1",3.4f);
        campaigns.add("2",4f);


       rows = solrParams.getInt(CommonParams.ROWS);
       Map<String,Object> mapParams = new HashMap<>();
       solrParams.toMap(mapParams);

        //TODO this might have to go into a custom queryparser
       mapParams.put(CommonParams.ROWS,40);
       //mapParams.put(CommonParams.FL,FIELD_TO_APPEND+",domain");
       mapParams.put(GroupParams.GROUP,true);
       mapParams.put(GroupParams.GROUP_FIELD,"domain");
       mapParams.put(GroupParams.GROUP_LIMIT,rows);

       Log.info("Sorting: "+solrParams.get(CommonParams.SORT));

        rb.req.setParams(new MapSolrParams(mapParams.entrySet().stream()
                .collect(Collectors.toMap(x-> x.getKey(), x-> String.valueOf(x.getValue())))));

        transformFactory = new CampaingScoreTransformFactory();
        transformFactory.init(campaigns);


        Collection<SchemaField> fields = rb.req.getSchema().getFields().values();
        fls = fields.stream().map(c->c.getName()).collect(Collectors.toSet());
    }

    public void process(ResponseBuilder rb) throws IOException {

        DocTransformer docTransformer = transformFactory.create(FIELD_TO_APPEND,rb.req.getParams(),rb.req);

        SolrParams solrParams= rb.req.getParams();

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


        GroupingSpecification groupSpecs = rb.getGroupingSpec();

        if(groupSpecs !=null)
            Log.info("Group specs not null");
        if(groupSpecs == null)
                Log.info("groups secs null");

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

        Set<Document> DOCS = new HashSet<>();

        Map<String,Object> dataMap = grouped.asShallowMap();

        SolrDocumentFetcher docFetcher =  searcher.getDocFetcher();

        SolrDocumentList solrDocumentList = new SolrDocumentList();

        for(Map.Entry<String,Object> entry: dataMap.entrySet())
        {
            Log.info("Loop 1: ");
           // Log.info("Key: "+entry.getKey()+" value: "+entry.getValue());
            Map<String,Object> domains = (HashMap)entry.getValue();


            int index =0;
            for(Map.Entry<String,Object> obj:domains.entrySet())
            {

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
                                    index = Math.min(index,SCORE_ARRAY.length-1);
                                    Log.info("Index "+index);
                                    int docsetid = iterator.nextDoc();
                                    float sc = docs.hasScores()? iterator.score():1.0f;

                                    Log.info("the score is: "+sc);

                                    SolrDocument docBase = new SolrDocument();
                                    docFetcher.decorateDocValueFields(docBase,docsetid,fls);
                                    Document d = searcher.doc(i,fls);

                                    for(IndexableField field:d.getFields())
                                    {
                                        docBase.addField(field.name(),field.stringValue());
                                    }

                                    ((CampaingScoreTransformFactory.CampaingsScoreTransformer) docTransformer)
                                            .transform(docBase,sc,SCORE_ARRAY[index++]);

                                    solrDocumentList.add(docBase);
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

        Collections.sort(solrDocumentList,Collections.reverseOrder((o1,o2)-> Float.compare((Float)o1.getFieldValue(FIELD_TO_APPEND),(Float)o2.getFieldValue(FIELD_TO_APPEND))));

        NamedList<Object> responseData = new NamedList<>();
        responseData.add("featured ads",solrDocumentList);
        responseData.add("normal ads", new SolrDocumentList());

        QueryResult rslt = searcher.search(queryResult,rb.createQueryCommand());
        ((SimpleOrderedMap) queryResult.groupedResults).clear();
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
        rb.rsp.addResponse(responseData);

        }

    public String getDescription() {
        return "CustomGroupingSearch";
    }


    public NamedList<Object> getStatistics(){
        return null;
   }

}
