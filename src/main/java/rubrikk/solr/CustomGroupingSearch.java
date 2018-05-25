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

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

public class CustomGroupingSearch extends SearchComponent
{
    private static final String FIELD_TO_APPEND = "GroupOrderScore";
    private static CampaignScoreTransformFactory TransformFactory;
    private static boolean UseRubrikkGrouping=false;
    private static float[] PlacementOrderScore;

    private Integer Rows;
    private Set<String> FieldList;

    //for stats
    volatile long totalRequestTime;

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

        TransformFactory = new CampaignScoreTransformFactory();

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

        long lstartTime = System.currentTimeMillis();

        if(!UseRubrikkGrouping){
            totalRequestTime+=System.currentTimeMillis()-lstartTime;
            return;
        }

        DocTransformer docTransformer = TransformFactory.create(FIELD_TO_APPEND,rb.req.getParams(),rb.req);

        SolrParams solrParams = rb.req.getParams();

        Map<String,Object> mapParams = new HashMap<>();
        solrParams.toMap(mapParams);

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

        SolrDocumentList solrDocumentListFeatured = getSolrDocuments((CampaignScoreTransformFactory.CampaignsScoreTransformer) docTransformer, searcher, queryResultFeatured,true);
        solrDocumentListFeatured.setNumFound(solrDocumentListFeatured.size());

        SolrDocumentList solrDocumentListNormal = getSolrDocuments((CampaignScoreTransformFactory.CampaignsScoreTransformer) docTransformer, searcher, queryResultNormal,false);
        solrDocumentListNormal.setNumFound(solrDocumentListNormal.size());

        Collections.sort(solrDocumentListFeatured,Collections.reverseOrder((o1,o2)-> Float.compare((Float)o1.getFieldValue(FIELD_TO_APPEND),(Float)o2.getFieldValue(FIELD_TO_APPEND))));
        Collections.sort(solrDocumentListNormal,Collections.reverseOrder((o1,o2)-> Float.compare((Float)o1.getFieldValue(FIELD_TO_APPEND),(Float)o2.getFieldValue(FIELD_TO_APPEND))));

        SolrDocumentList finalSolrDocList = appendResults(solrDocumentListFeatured,solrDocumentListNormal);

        finalSolrDocList.setNumFound(solrDocumentListFeatured.size()+solrDocumentListNormal.size());

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

    private SolrDocumentList getSolrDocuments(CampaignScoreTransformFactory.CampaignsScoreTransformer docTransformer, SolrIndexSearcher searcher, QueryResult queryResultFeatured, boolean boostCampaigns) throws IOException {

        SolrDocumentFetcher docFetcher =  searcher.getDocFetcher();

        SolrDocumentList solrDocumentListFeatured = new SolrDocumentList();

        IndexSchema schema = searcher.getSchema();

        Map grouped = ((SimpleOrderedMap) queryResultFeatured.groupedResults).asShallowMap();

        Map domains = ((SimpleOrderedMap)grouped.get("domain")).asShallowMap();

        ArrayList<Object> groups = (ArrayList<Object>)domains.get("groups");

        for (Object group:groups)
        {

            DocSlice docSlice = (DocSlice)((SimpleOrderedMap)group).get("doclist");

            int index = 0;

            DocIterator iterator = docSlice.iterator();
            
            for(int i =0; i<docSlice.size(); i++)
            {
                index = Math.min(index,PlacementOrderScore.length-1);

                int docSetId = iterator.nextDoc();

                SolrDocument docBase = new SolrDocument();

                docFetcher.decorateDocValueFields(docBase,docSetId,FieldList);

                docBase.addField("score",iterator.score());
                
                Document doc = searcher.doc(i,FieldList);

                for(IndexableField field:doc.getFields())
                {
                    FieldType type = schema.getFieldType(field.name());

                    Object fieldvalue = type.toObject(field);

                    if (fieldvalue instanceof UUID)
                        docBase.addField(field.name(),field.stringValue());

                    else
                        docBase.addField(field.name(),fieldvalue);
                }

                docTransformer.transform(docBase,iterator.score(),PlacementOrderScore[index++],boostCampaigns);

                String documentType = boostCampaigns ? "featured" : "normal";

                docTransformer.appendField(docBase,"adType",documentType);

                solrDocumentListFeatured.add(docBase);
            }
        }
        return solrDocumentListFeatured;
    }

    private SolrDocumentList appendResults(SolrDocumentList featured, SolrDocumentList normal)
    {
        SolrDocumentList finalList = new SolrDocumentList();

        finalList.addAll(featured.subList(0, Math.min(featured.size(),Rows) ) );
        finalList.addAll(normal.subList(0, Math.min(normal.size(),Rows) ) );

        return  finalList;
    }

    private void PerformGroupSearch(ResponseBuilder rb, SolrIndexSearcher searcher, QueryResult queryResult, QueryCommand queryCommand, GroupingSpecification groupSpecs) throws IOException
    {
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
    }

    public String getDescription() {
        return "CustomGroupingSearch";
    }

    public NamedList<Object> getStatistics(){
        NamedList stats = new SimpleOrderedMap<Object>();
        stats.add("totalTime(ms)",""+totalRequestTime);
        return stats;
   }
}

