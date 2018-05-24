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
import org.apache.solr.request.SolrQueryRequest;
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
import java.util.stream.Collectors;

public class CustomGroupingSearch extends SearchComponent  {

    private static final Logger Log = LoggerFactory.getLogger(CustomGroupingSearch.class);
    private static final String FIELD_TO_APPEND = "GroupOrderScore";
    private static CampaingScoreTransformFactory TransformFactory;
    private static boolean UseRubrikkGrouping=false;
    private Integer Rows;
    private Set<String> FieldList;
    private static float[] PlacementOrderScore;
    
    
    @Override
    public void init(NamedList args){

        super.init(args);
        List<Float> placementOrderScore = ((NamedList)args.get("placementScore")).getAll("score");
        if(placementOrderScore !=null)
            PlacementOrderScore = ArrayUtils.toPrimitive(placementOrderScore.toArray(new Float[0]));
    }

    public void prepare(ResponseBuilder rb) throws IOException {

        SolrParams solrParams = rb.req.getParams();

        if(solrParams.getBool("RubrikkGrouping")!=null)
            UseRubrikkGrouping = solrParams.getBool("RubrikkGrouping");

        Rows = solrParams.getInt(CommonParams.ROWS);

        Map<String,Object> mapParams = new HashMap<>();
        solrParams.toMap(mapParams);

        TransformFactory = new CampaingScoreTransformFactory();

        ObjectMapper objectMapper = new ObjectMapper();

        if(solrParams.get("Campaign") != null){
            NamedList campaigns = new NamedList<>(objectMapper.readValue(solrParams.get("Campaign"),HashMap.class));
            campaigns.add("0",1.0f);

            TransformFactory.init(campaigns);
        }

        Collection<SchemaField> fields = rb.req.getSchema().getFields().values();
        FieldList = fields.stream().map(SchemaField::getName).collect(Collectors.toSet());
    }

    public void process(ResponseBuilder rb) throws IOException {

        if(!UseRubrikkGrouping){
            return;
        }

        DocTransformer docTransformer = TransformFactory.create(FIELD_TO_APPEND,rb.req.getParams(),rb.req);

        SolrParams solrParams = rb.req.getParams();

        Map<String,Object> mapParams = new HashMap<>();
        solrParams.toMap(mapParams);

        Log.info("Sorting: "+solrParams.get(CommonParams.SORT));

        rb.req.setParams(new MapSolrParams(mapParams.entrySet().stream()
                .collect(Collectors.toMap(x-> x.getKey(), x-> String.valueOf(x.getValue())))));

        SolrIndexSearcher searcher = rb.req.getSearcher();

        List<Query> filters = rb.getFilters();
        List<Query> normalFilters = new ArrayList<>();

        QueryResult queryResultFeatured = new QueryResult();
        
        QueryResult queryResultNormal = new QueryResult();

       QueryCommand queryCommandFeatured = rb.createQueryCommand();

       QueryCommand queryCommandNormal = rb.createQueryCommand();

       if(filters !=null){
           for(Query query:filters){
               if(!query.toString().contains("Campaign_id")){
                   normalFilters.add(query);
               }
           }
       }

       queryCommandNormal.setFilterList(normalFilters);

       GroupingSpecification groupSpecs = new GroupingSpecification();

       prepareGrouping(rb, groupSpecs);


        PerformGroupSearch(rb, searcher, queryResultFeatured, queryCommandFeatured, groupSpecs);
        
        PerformGroupSearch(rb, searcher, queryResultNormal, queryCommandNormal, groupSpecs);

        SolrDocumentList solrDocumentListFeatured = getSolrDocuments((CampaingScoreTransformFactory.CampaignsScoreTransformer) docTransformer, searcher, queryResultFeatured,true);

        solrDocumentListFeatured.setNumFound(solrDocumentListFeatured.size());

        //TODO don't boost on camapaign ids at all here
        SolrDocumentList solrDocumentListNormal = getSolrDocuments((CampaingScoreTransformFactory.CampaignsScoreTransformer) docTransformer, searcher, queryResultNormal,false);

        solrDocumentListNormal.setNumFound(solrDocumentListNormal.size());

        Collections.sort(solrDocumentListFeatured,Collections.reverseOrder((o1,o2)-> Float.compare((Float)o1.getFieldValue(FIELD_TO_APPEND),(Float)o2.getFieldValue(FIELD_TO_APPEND))));

        Collections.sort(solrDocumentListNormal,Collections.reverseOrder((o1,o2)-> Float.compare((Float)o1.getFieldValue(FIELD_TO_APPEND),(Float)o2.getFieldValue(FIELD_TO_APPEND))));

        SolrDocumentList finalSolrDocList = new SolrDocumentList();

        finalSolrDocList.addAll(solrDocumentListFeatured);

        finalSolrDocList.addAll(solrDocumentListNormal);

        finalSolrDocList.setNumFound(solrDocumentListFeatured.size()+solrDocumentListNormal.size());

        NamedList<Object> response = rb.rsp.getValues();
        for (int i = 0; i < response.size();++i)
        {
            if(response.getVal(i) instanceof BasicResultContext){
                response.remove(i);
            }
        }

        rb.rsp.addResponse(finalSolrDocList);
        }


    private void prepareGrouping(ResponseBuilder rb,GroupingSpecification groupingSpec) throws IOException
    {
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
        withinGroupSortStr.setCount(Rows);

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

    private SolrDocumentList getSolrDocuments(CampaingScoreTransformFactory.CampaignsScoreTransformer docTransformer, SolrIndexSearcher searcher, QueryResult queryResultFeatured, boolean boostCampaigns) throws IOException {

        SolrDocumentFetcher docFetcher =  searcher.getDocFetcher();

        SolrDocumentList solrDocumentListFeatured = new SolrDocumentList();

        IndexSchema schema = searcher.getSchema();

        Map grouped = ((SimpleOrderedMap) queryResultFeatured.groupedResults).asShallowMap();

        Map domains = ((SimpleOrderedMap)grouped.get("domain")).asShallowMap();

        ArrayList<Object> groups = (ArrayList<Object>)domains.get("groups");

        for (Object group:groups)
        {

            DocSlice docSlice =  (DocSlice)((SimpleOrderedMap)group).get("doclist");

            int index =0;

            //TODO row len might be not what we need
            DocList docs = docSlice.subset(0,Rows);
            DocIterator iterator = docs.iterator();

            Log.info("docs.size "+docs.size());
            for(int i =0; i<docs.size(); i++)
            {
                index = Math.min(index,PlacementOrderScore.length-1);
                Log.info("Index "+index);
                int docsetid = iterator.nextDoc();

                SolrDocument docBase = new SolrDocument();

                docFetcher.decorateDocValueFields(docBase,docsetid,FieldList);
                Document d = searcher.doc(i,FieldList);

                docBase.addField("score",iterator.score());

                for(IndexableField field:d.getFields())
                {

                    FieldType type = schema.getFieldType(field.name());

                    Object fieldvalue = type.toObject(field);

                    if (fieldvalue instanceof UUID)
                        docBase.addField(field.name(),field.stringValue());

                    else
                        docBase.addField(field.name(),fieldvalue);
                }

                docTransformer
                        .transform(docBase,iterator.score(),PlacementOrderScore[index++],boostCampaigns);
                String documentType = boostCampaigns ? "featured" : "normal";

                docTransformer.appendField(docBase,"adType",documentType);

                solrDocumentListFeatured.add(docBase);
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

        try {
            grouping.addFieldCommand("domain", rb.req);
        }
        catch (SyntaxError syntaxError)
        {
            syntaxError.printStackTrace();
        }


        if (groupSpecs.getFunctions() != null)
            for (String groupByStr : groupSpecs.getFunctions()) {
                try {
                    grouping.addFunctionCommand(groupByStr, rb.req);
                } catch (SyntaxError syntaxError) {
                    syntaxError.printStackTrace();
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
}

