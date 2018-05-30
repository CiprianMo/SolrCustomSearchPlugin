package rubrikk.solr;

import org.apache.commons.lang.ArrayUtils;
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
import org.apache.solr.handler.component.ResponseBuilder;
import org.apache.solr.handler.component.SearchComponent;
import org.apache.solr.response.BasicResultContext;
import org.apache.solr.response.transform.DocTransformer;
import org.apache.solr.schema.FieldType;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.search.*;
import org.apache.solr.search.grouping.GroupingSpecification;

import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

public class CustomGroupingSearch extends SearchComponent
{
    private static final Logger Log = LoggerFactory.getLogger(CustomGroupingSearch.class);
    private static final String FIELD_TO_APPEND = "GroupOrderScore";
    private CampaignScoreTransformFactory transformFactory;
    private boolean useRubrikkGrouping;
    private float[] placementOrderScore;

    private List<String> campaignIds;

    private Integer rows;
    private Integer withinGroupRows = 1;
    private Set<String> fieldList;

    //for stats
    volatile long totalRequestTime;

    @Override
    public void init(NamedList args){

        super.init(args);
        List<Float> placementOrderScoreList = ((NamedList)args.get("placementScore")).getAll("score");
        if(placementOrderScoreList !=null)
            placementOrderScore = ArrayUtils.toPrimitive(placementOrderScoreList.toArray(new Float[0]));
    }

    public void prepare(ResponseBuilder rb) throws IOException {

        SolrParams solrParams = rb.req.getParams();
        useRubrikkGrouping = false;

        if(solrParams.getBool("RubrikkGrouping")!=null)
            useRubrikkGrouping = solrParams.getBool("RubrikkGrouping");

        if(solrParams.getInt("RubrikkGrouping.limit") !=null)
            withinGroupRows = solrParams.getInt("RubrikkGrouping.limit");

        rows = solrParams.getInt(CommonParams.ROWS);

        Map<String,Object> mapParams = new HashMap<>();
        solrParams.toMap(mapParams);

        transformFactory = new CampaignScoreTransformFactory();

        ObjectMapper objectMapper = new ObjectMapper();

        if(solrParams.get("Campaign") != null){
            NamedList campaigns = new NamedList<>(objectMapper.readValue(solrParams.get("Campaign"),HashMap.class));

            transformFactory.init(campaigns);
            campaignIds = new ArrayList<String>(campaigns.asShallowMap().keySet());
        }


        fieldList = new HashSet<>();
        fieldList.add("ID");

    }

    public void process(ResponseBuilder rb) throws IOException {

        long lstartTime = System.currentTimeMillis();
        Log.debug("RubrikkGrouping "+useRubrikkGrouping);
        if(!useRubrikkGrouping){

            totalRequestTime+=System.currentTimeMillis()-lstartTime;
            return;
        }

        SolrParams solrParams = rb.req.getParams();

        DocTransformer docTransformer = transformFactory.create(FIELD_TO_APPEND,rb.req.getParams(),rb.req);

        Map<String,Object> mapParams = new HashMap<>();
        solrParams.toMap(mapParams);

        rb.req.setParams(new MapSolrParams(mapParams.entrySet().stream()
                .collect(Collectors.toMap(x-> x.getKey(), x-> String.valueOf(x.getValue())))));

        SolrIndexSearcher searcher = rb.req.getSearcher();

        QueryCommand queryCommandFeatured = rb.getQueryCommand();
        QueryCommand queryCommandNormal = rb.getQueryCommand();

        List<Query> normalFilters = rb.getFilters().stream().filter(e->!e.toString().contains("Campaign_id")).collect(Collectors.toList());
        queryCommandNormal.setFilterList(normalFilters);

        for(Query query: queryCommandNormal.getFilterList())
        {
            Log.debug("normal filter: "+query.toString());
        }

        GroupingSpecification groupSpecs = new GroupingSpecification();

        prepareGrouping(rb, groupSpecs);

        totalRequestTime+=System.currentTimeMillis()-lstartTime;
        Log.debug("Time before grouping: "+totalRequestTime);

        CompletableFuture<QueryResult> queryResultFeatureFuture = CompletableFuture.supplyAsync(()->{
            try {
                return performGroupSearch(rb, searcher, queryCommandFeatured, groupSpecs);
            } catch (IOException e) {
                e.printStackTrace();
                return null;
            }
        });

        CompletableFuture<SolrDocumentList> solrDocumentListFeaturedFuture = queryResultFeatureFuture.thenApply( queryR->{
            try {
                return getSolrDocuments((CampaignScoreTransformFactory.CampaignsScoreTransformer) docTransformer, searcher, queryR,true);
            } catch (IOException e) {
                e.printStackTrace();
                return null;
            }
        });

        CompletableFuture<SolrDocumentList> solrDocumentListFeaturedSorted = solrDocumentListFeaturedFuture.thenApply(list ->{
                list.sort((o1,o2) -> Float.compare((float)o2.getFieldValue(FIELD_TO_APPEND), (float)o1.getFieldValue(FIELD_TO_APPEND)));
                return list;
            });

        CompletableFuture<QueryResult> queryResultNormalFuture = CompletableFuture.supplyAsync(()->{
            try {
                return performGroupSearch(rb, searcher, queryCommandNormal, groupSpecs);
            } catch (IOException e) {
                e.printStackTrace();
                return null;
            }
        });

        CompletableFuture<SolrDocumentList> solrDocumentListNormalFuture = queryResultNormalFuture.thenApply( queryR-> {
            try {
                return getSolrDocuments((CampaignScoreTransformFactory.CampaignsScoreTransformer) docTransformer, searcher, queryR,false);
            } catch (IOException e) {
                e.printStackTrace();
                return null;
            }
        });

        CompletableFuture<SolrDocumentList> solrDocumentListNormalFutureSorted = solrDocumentListNormalFuture.thenApply(list->{
            list.sort((o1,o2) -> Float.compare((float)o2.getFieldValue(FIELD_TO_APPEND), (float)o1.getFieldValue(FIELD_TO_APPEND)));
            return list;
        });

        CompletableFuture<SolrDocumentList> combinedResultFuture =  solrDocumentListFeaturedSorted.thenCombine(solrDocumentListNormalFutureSorted,(featured,normal)->{
            Long numfound = featured.getNumFound() + normal.getNumFound();
            SolrDocumentList finalList = appendResults(featured,normal);
            finalList.setNumFound(numfound);
            return finalList;
        });

        SolrDocumentList finalSolrDocList = null;

        try {
            finalSolrDocList = combinedResultFuture.get();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
        totalRequestTime+=System.currentTimeMillis()-lstartTime;
        Log.debug("Time after grouping: "+totalRequestTime);

        Collection<SchemaField> fields = rb.req.getSchema().getFields().values();
        Set<String>remainingFields= fields.stream().map(SchemaField::getName).collect(Collectors.toSet());

        fillUpDocs(finalSolrDocList,remainingFields,searcher);

        totalRequestTime+=System.currentTimeMillis()-lstartTime;
        Log.debug("Time after filling up data: "+totalRequestTime);

        //Cleaning up the original response and replacing with our custom response
        NamedList<Object> response = rb.rsp.getValues();
        for (int i = 0; i < response.size();++i)
        {
            if(response.getVal(i) instanceof BasicResultContext){
                response.remove(i);
            }
        }

        rb.rsp.addResponse(finalSolrDocList);
        totalRequestTime+=System.currentTimeMillis()-lstartTime;
        Log.debug("End time: "+totalRequestTime);
    }

    private void fillUpDocs (SolrDocumentList docList, Set<String> fields,SolrIndexSearcher searcher) throws IOException {
        SolrDocumentFetcher docFetcher =  searcher.getDocFetcher();

        for(int i =0; i<docList.size(); i++)
        {
            SolrDocument docBase = docList.get(i);
            int docId = (Integer) docBase.getFieldValue("luceneId");
            docFetcher.decorateDocValueFields(docBase,docId,fields);
        }
    }

    private void prepareGrouping(ResponseBuilder rb,GroupingSpecification groupingSpec) throws IOException
    {
        SolrIndexSearcher searcher = rb.req.getSearcher();

        final SortSpec sortSpec = rb.getSortSpec();

        final SortSpec groupSortSpec = searcher.weightSortSpec(sortSpec, Sort.RELEVANCE);

        SortSpec withinGroupSortStr = new SortSpec(
                groupSortSpec.getSort(),
                groupSortSpec.getSchemaFields(),
                groupSortSpec.getCount(),
                groupSortSpec.getOffset());
        withinGroupSortStr.setOffset(0);
        withinGroupSortStr.setCount(withinGroupRows);

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

    private SolrDocumentList getSolrDocuments(CampaignScoreTransformFactory.CampaignsScoreTransformer docTransformer, SolrIndexSearcher searcher, QueryResult queryResultFeatured, boolean boostCampaigns) throws IOException {

        Set<String> docValFields = new HashSet<>();
        docValFields.add("Campaign_id");
        docValFields.add("Quality_boost");
        docValFields.add("domain");

        SolrDocumentFetcher docFetcher =  searcher.getDocFetcher();

        SolrDocumentList solrDocumentList = new SolrDocumentList();

        IndexSchema schema = searcher.getSchema();

        Map grouped = ((SimpleOrderedMap) queryResultFeatured.groupedResults).asShallowMap();

        Map domains = ((SimpleOrderedMap)grouped.get("domain")).asShallowMap();

        ArrayList<Object> groups = (ArrayList<Object>)domains.get("groups");
        solrDocumentList.setNumFound(Long.valueOf(String.valueOf(domains.get("matches"))));

        for (Object group:groups)
        {

            DocSlice docSlice = (DocSlice)((SimpleOrderedMap)group).get("doclist");

            int index = 0;

            DocIterator iterator = docSlice.iterator();
            
            for(int i =0; i<docSlice.size(); i++)
            {
                index = Math.min(index,placementOrderScore.length-1);

                int docSetId = iterator.nextDoc();

                SolrDocument docBase = new SolrDocument();

                docFetcher.decorateDocValueFields(docBase,docSetId,docValFields);

                docBase.addField("score",iterator.score());
                docBase.addField("luceneId",docSetId);

                Document doc = searcher.doc(docSetId,fieldList);

                for(IndexableField field:doc.getFields())
                {
                    FieldType type = schema.getFieldType(field.name());

                    Object fieldvalue = type.toObject(field);

                    if (fieldvalue instanceof UUID)
                        docBase.addField(field.name(),field.stringValue());

                    else
                        docBase.addField(field.name(),fieldvalue);
                }

                docTransformer.transform(docBase,iterator.score(),placementOrderScore[index++],boostCampaigns);

                String documentType = boostCampaigns ? "featured" : "normal";

                docTransformer.appendField(docBase,"adType",documentType);

                solrDocumentList.add(docBase);
            }
        }
        return solrDocumentList;
    }

    private SolrDocumentList appendResults(SolrDocumentList featured, SolrDocumentList normal)
    {
        SolrDocumentList finalList = new SolrDocumentList();

        finalList.addAll(featured.subList(0, Math.min(featured.size(),rows) ) );
        finalList.addAll(normal.subList(0, Math.min(normal.size(),rows) ) );

        return  finalList;
    }

    private QueryResult performGroupSearch(ResponseBuilder rb, SolrIndexSearcher searcher, QueryCommand queryCommand, GroupingSpecification groupSpecs) throws IOException
    {
        QueryResult queryResult = new QueryResult();
        Grouping grouping = new Grouping(searcher,queryResult,queryCommand,false,0,false );

        grouping.setGroupSort(rb.getSortSpec().getSort())
                .setWithinGroupSort(groupSpecs.getWithinGroupSortSpec().getSort())
                .setDefaultFormat(groupSpecs.getResponseFormat())
                .setLimitDefault(queryCommand.getLen())
                .setDocsPerGroupDefault(groupSpecs.getWithinGroupSortSpec().getCount())
                .setGroupOffsetDefault(0)
                .setGetGroupedDocSet(groupSpecs.isTruncateGroups());

        try {
            grouping.addFieldCommand("domain", rb.req);
        }
        catch (SyntaxError syntaxError)
        {
            syntaxError.printStackTrace();
        }

        if( rb.isNeedDocList() || rb.isDebug() ){
            queryCommand.setFlags(SolrIndexSearcher.GET_DOCLIST);
        }

        //TODO check if this fist of second pass score
        queryCommand.setFlags(SolrIndexSearcher.GET_SCORES);
        grouping.execute();

        return queryResult;
    }

    public String getDescription() {
        return "CustomGroupingSearch";
    }

    @Override
    public NamedList<Object> getStatistics(){
        NamedList stats = new SimpleOrderedMap<Object>();
        stats.add("Custom stats totalTime(ms)",""+totalRequestTime);
        return stats;
   }
}

