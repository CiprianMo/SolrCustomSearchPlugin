package rubrikk.solr;

import org.apache.commons.lang.ArrayUtils;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.search.Sort;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.*;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.handler.component.*;
import org.apache.solr.schema.FieldType;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.search.*;

import org.codehaus.jackson.map.ObjectMapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

import java.util.stream.Collectors;

import static java.lang.Integer.parseInt;

//extending the QueryComponent to let that do the actual search
public class CustomGroupingSearch extends QueryComponent
{
    private static final Logger Log = LoggerFactory.getLogger(CustomGroupingSearch.class);
    private static final String FIELD_TO_APPEND = "GroupOrderScore";
    private static final String RUBRIKK_GROUPING = "RubrikkGrouping";

    private boolean useRubrikkGrouping = false;

    private ObjectMapper objectMapper;

    private static float[] placementOrderScore;
    private CampaignScoreTransformFactory transformFactory = new CampaignScoreTransformFactory();

    private List<Integer> campaignIds;

    private Integer rows;
    private Integer withinGroupRows = 24;

    @Override
    public void init(NamedList args){
        long lstartTime = System.currentTimeMillis();
        long totalRequestTime=0;
        List<Float> placementOrderScoreList = ((NamedList)args.get("placementScore")).getAll("score");
        if(placementOrderScoreList !=null)
            placementOrderScore = ArrayUtils.toPrimitive(placementOrderScoreList.toArray(new Float[0]));

         new ObjectMapper();

        totalRequestTime+=System.currentTimeMillis()-lstartTime;
        Log.info("init qtime "+totalRequestTime);


    }

    @Override
    public void prepare(ResponseBuilder rb) throws IOException {
        long lstartTime = System.currentTimeMillis();
        long totalRequestTime=0;
        useRubrikkGrouping = false;

        SolrParams solrParams = rb.req.getParams();

        rows = solrParams.getInt(CommonParams.ROWS);

        if(solrParams.getBool(RUBRIKK_GROUPING)!=null)
            useRubrikkGrouping = solrParams.getBool(RUBRIKK_GROUPING);

        if(rb.req.getParams().getBool(ShardParams.IS_SHARD) !=null && rb.req.getParams().getBool(ShardParams.IS_SHARD) == true){
            Log.info("is sharded");
        }

        if(!useRubrikkGrouping || (rb.req.getParams().getBool(ShardParams.IS_SHARD) !=null && rb.req.getParams().getBool(ShardParams.IS_SHARD) == true )){
            return;
        }

        if(solrParams.get(GroupParams.GROUP)!=null ||
                solrParams.get(GroupParams.GROUP_LIMIT)!=null||
                solrParams.get(GroupParams.GROUP_MAIN) !=null||
                solrParams.get(GroupParams.GROUP_FIELD)!=null||
                solrParams.get(GroupParams.GROUP_FORMAT)!=null)
        {
            throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
                    "Grouping parameters are not supported together with RubrikkGrouping parameters");
        }

        if(solrParams.getInt("RubrikkGrouping.limit") !=null)
            withinGroupRows = solrParams.getInt("RubrikkGrouping.limit");

        // we need to clean this list from the previous search
        campaignIds = new ArrayList<>();

        if(solrParams.get("Campaign") != null){
            NamedList campaigns = new NamedList<>(objectMapper.readValue(solrParams.get("Campaign"),HashMap.class));

            transformFactory.init(campaigns);
            Set<String> idsString = campaigns.asShallowMap().keySet();
            campaignIds = new ArrayList<>(idsString.stream().map(o-> parseInt((o))).collect(Collectors.toSet()));
        }
        totalRequestTime+=System.currentTimeMillis()-lstartTime;
        Log.info("main prepare method qtime "+totalRequestTime);
    }

    @Override
    public void process(ResponseBuilder rb) throws IOException {

        long lstartTime = System.currentTimeMillis();
        long totalRequestTime=0;

        if(!useRubrikkGrouping || (rb.req.getParams().getBool(ShardParams.IS_SHARD) !=null && rb.req.getParams().getBool(ShardParams.IS_SHARD) == true )){
            return;
        }
        Map<String,SolrParams> allSolrParams = new HashMap<>();
                NamedList params = rb.req.getParams().toNamedList();

        //cleaning the Rubrikk solr parameters and adding grouping parameters.
        parseNormalParams(params);

        //adding the solr parameters for the normal search
        allSolrParams.put("normal",SolrParams.toSolrParams(params));

        if(!campaignIds.isEmpty()) {

            NamedList featuredParams = addFeaturedParams(params,campaignIds);

            //adding solr parameters for the customer campaigns search
            allSolrParams.put("featured",SolrParams.toSolrParams(featuredParams));
        }

        // try to perform the searches in parallel only if we have campaigns, to look for
        // if not skip the overhead of creating parallel logic
        if (allSolrParams.size() > 1){

            allSolrParams.entrySet().stream().parallel().forEach(paramEntry->{

                rb.req.setParams(paramEntry.getValue());
                try {
                    processQuery(rb,paramEntry.getKey());
                } catch (IOException e) {
                    e.printStackTrace();
                }
            });
        }
        else{

            //extract only the first set of solr parameters, which corresponds to the normal search
            Map.Entry<String,SolrParams> paramEntry = allSolrParams.entrySet().iterator().next();

            rb.req.setParams(paramEntry.getValue());
            try {
                processQuery(rb,paramEntry.getKey());
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        //at this moment the SolrQueryResponse in rb will contain 2 grouped responses with
        //here we are flattening and combining the groups into one response
        createResponse(rb);

        totalRequestTime+=System.currentTimeMillis()-lstartTime;
        Log.info("main process method qtime "+totalRequestTime);
    }

    public NamedList addFeaturedParams(NamedList params, List<Integer> campaignIds){

        NamedList featured = new NamedList(params.asShallowMap());
        String campaignFilter = new String();

        for(int campaignid: campaignIds){
            campaignFilter+="Campaign_id:["+campaignid+" TO "+campaignid+"] OR ";
        }

        if(campaignFilter.endsWith(" OR ")){
            campaignFilter=campaignFilter.substring(0,campaignFilter.length()-4);
        }

        Object fqs = featured.get(CommonParams.FQ);

        if(fqs instanceof String[])
        {
            List<String> filters = Arrays.asList((String[])fqs);
            String fqList = new String();
            for (String filter :filters){
                fqList+=filter+" AND ";
            }

            if(fqList.endsWith(" AND ")){
                fqList=fqList.substring(0,fqList.length()-5);
            }

            if (!campaignFilter.isEmpty()){

                String newFqs = fqList+" AND "+"("+campaignFilter+")";
                featured.remove(CommonParams.FQ);
                featured.add(CommonParams.FQ,newFqs);
            }
        }
        if(fqs instanceof String){

            if (!campaignFilter.isEmpty()){

                String newFqs = fqs+" AND "+"("+campaignFilter+")";
                featured.remove(CommonParams.FQ);
                featured.add(CommonParams.FQ,newFqs);
            }
        }
        else {
            Log.info("No original filters");
            if(!campaignFilter.isEmpty())
            {
                featured.add(CommonParams.FQ,campaignFilter);
            }
        }

        //adding boost
        featured.remove("boost");
        featured.add("boost","quality");

        return featured;
    }

    public void parseNormalParams(NamedList params){

        params.remove("Campaign");
        params.remove(RUBRIKK_GROUPING);

        for (int i =0;i < params.size(); i++){
            if(params.getVal(i) instanceof String[])
            {
                List<String> paramsList = Arrays.asList((String[])params.getVal(i));
                String paramsString = new String();
                for(String param: paramsList){
                    paramsString+= param+" AND ";
                }
                if(paramsString.endsWith(" AND ")){
                    paramsString = paramsString.substring(0,paramsString.length()-5);
                }
                params.setVal(i,paramsString);
            }
        }

        List<String> fls;

        if(params.get(CommonParams.FL) instanceof String[]){

            fls = new ArrayList<>(Arrays.asList((String[])params.get(CommonParams.FL)));

        }
        else{
            String flsString = (String)params.get(CommonParams.FL);
            String[] fields = flsString.split("[,\\s]+");
            fls = new ArrayList<>(Arrays.asList(fields));
        }

        if(!fls.contains("score")){
            fls.add("score");
            params.remove(CommonParams.FL);
            params.add(CommonParams.FL,String.join(", ",fls));
        }

        params.add("boost","Quality_boost");
        params.add(GroupParams.GROUP,true);
        params.add(GroupParams.GROUP_FIELD,"domain");
        params.add(GroupParams.GROUP_FORMAT,"grouped");
        params.add(GroupParams.GROUP_LIMIT,withinGroupRows);
    }

    protected void processQuery(ResponseBuilder rb,String searchMethod) throws IOException {

        long totalRequestTime = 0;
        long lstartTime = System.currentTimeMillis();

        rb.setQuery(null);
        rb.setQueryString(null);

        super.prepare(rb);
        super.process(rb);

        Object data = rb.rsp.getValues().get("grouped");

        NamedList grouped = (NamedList) data;
        if(grouped !=null){

            //Adding a new field to identify what type of search has been done
            grouped.add("SearchMethod",searchMethod);
        }
        rb.rsp.getValues().remove("grouped");

        rb.rsp.add("result",grouped);
        totalRequestTime+=System.currentTimeMillis()-lstartTime;
        Log.info(searchMethod+" method qtime "+totalRequestTime);
    }

    private void createResponse(ResponseBuilder rb) throws IOException {

        //This fields are needed right away because we need to calculate new type of score - GroupOrderScore
        Set<String> docValFields = new HashSet<>();
        docValFields.add("Campaign_id");
        docValFields.add("Quality_boost");
        docValFields.add("domain");
        docValFields.add("quality");

        NamedList response = rb.rsp.getValues();

        SolrDocumentList result = new SolrDocumentList();
        for (int i = 0; i < response.size();i++) {
            if (response.getName(i) == "result") {

                SolrDocumentList groupedResult = parseGroupedResults(response.getVal(i),rb,docValFields);

                groupedResult.sort((o1,o2) -> Float.compare((float)o2.getFieldValue("score"), (float)o1.getFieldValue("score")));

                if(groupedResult.size() > 0){

                    //get only the top (rows) documents from the flattened grouped results
                    result.addAll(groupedResult.subList(0, Math.min(rows,groupedResult.size())));
                }
                result.setNumFound(groupedResult.getNumFound());
            }
        }

        while(response.get("result")!=null){
            response.remove("result");
        }

        //we clean the response that comes from the default SearchComponent which is "QueryComponent"
        //since "QueryComponent" will run by default, it probably adds time to overall qtime
        response.remove("response");

        //we only want to fill up the rest of the field for the documents in the final result.
        fillUpDocs(result,rb,docValFields);
        response.add("response", result);
    }

    public SolrDocumentList deDuplicateBasedOnQuality(SolrDocumentList list){

        SolrDocumentList distinctList = new SolrDocumentList();
        if(list!=null)
        {
            distinctList.add(list.get(0));

            for (int i = 1; i < list.size(); i++) {
                SolrDocument doc = list.get(i);

                for(int j = 0; j<distinctList.size(); j++){
                    if(doc.getFieldValue("duplicatehash").equals(distinctList.get(j).getFieldValue("duplicatehash"))){
                        SolrDocument d = distinctList.get(j);
                        if((Float)d.getFieldValue("quality") < (Float)doc.getFieldValue("quality"))
                        {
                            distinctList.set(j,doc);
                        }
                        Log.info("duplicate found");
                    }
                    else{
                        distinctList.add(doc);
                        break;
                    }
                }
            }
        }

        return distinctList;
    }

    public SolrDocumentList parseGroupedResults(Object data,ResponseBuilder rb, Set<String> docValFields){

        SolrDocumentList solrDocumentList = new SolrDocumentList();

        NamedList grouped = (NamedList) data;
        if(data !=null){
            NamedList domains = (NamedList)grouped.get("domain");
            String searchMethod = (String)grouped.get("SearchMethod");

            ArrayList<Object> groups = (ArrayList<Object>)domains.get("groups");
            solrDocumentList.setNumFound(Long.valueOf(String.valueOf(domains.get("matches"))));

            for(Object group:groups){
                if (group !=null){

                    DocSlice docSlice = (DocSlice)((SimpleOrderedMap)group).get("doclist");

                    parseGroup(docSlice,rb,searchMethod, solrDocumentList,docValFields);
                }
            }
        }

        return solrDocumentList;
    }

    public SolrDocumentList parseGroup(DocSlice docSlice, ResponseBuilder rb, String searchMethod,SolrDocumentList docList, Set<String> docValFields){

        int index = 0;
        DocIterator iterator = docSlice.iterator();

        SolrDocumentFetcher docFetcher =  rb.req.getSearcher().getDocFetcher();
        for(int i =0; i<docSlice.size(); i++){
            index = Math.min(index,placementOrderScore.length-1);

            int docSetId = iterator.nextDoc();

            SolrDocument docBase = new SolrDocument();
            try {
                docFetcher.decorateDocValueFields(docBase,docSetId,docValFields);
            } catch (IOException e) {
                Log.error("DecoradeDocValues "+e.getMessage());
            }

            docBase.addField("score",iterator.score());
            docBase.addField("luceneId",docSetId);

            Set<String> fieldList = new HashSet<>();
            fieldList.add("ID");
            fieldList.add("quality");

            Document doc = null;
            try {
                doc = rb.req.getSearcher().doc(docSetId,fieldList);
            } catch (IOException e) {
                Log.error("getting doc data "+e.getMessage());
            }

            for(IndexableField field:doc.getFields()) {
                FieldType type = rb.req.getSchema().getFieldType(field.name());

                Object fieldvalue = type.toObject(field);

                if (fieldvalue instanceof UUID)
                    docBase.addField(field.name(), field.stringValue());

                else
                    docBase.addField(field.name(), fieldvalue);
            }

            //TODO probably doesn't have to be a Tansformer object
            CampaignScoreTransformFactory.CampaignsScoreTransformer docTransformer = transformFactory.
                    create(FIELD_TO_APPEND,rb.req.getParams(),rb.req);

            docTransformer.transform(docBase,iterator.score(),placementOrderScore[index++],(searchMethod=="featured"));

            docTransformer.appendField(docBase,"SearchMethod",searchMethod);

            docList.add(docBase);
        }

        return docList;
    }

    private void fillUpDocs (SolrDocumentList docList,ResponseBuilder rb,Set<String> docValFields) throws IOException {

        Collection<SchemaField> fields = rb.req.getSchema().getFields().values();

        String fl = rb.req.getParams().get(CommonParams.FL);

        Set<String> remainingFields = new HashSet<>();

        if(fl.equals("*,score") || fl.equals("*")){

            remainingFields = fields.stream().map(SchemaField::getName).filter(o-> !docValFields.contains(o)).collect(Collectors.toSet());
        }
        else
        {
            List<String>requestedFields = new ArrayList<>(Arrays.asList(fl.split("[,\\s]+")));


            if(! requestedFields.isEmpty() )
            {
                remainingFields = requestedFields.stream().filter(o-> !docValFields.contains(o)).collect(Collectors.toSet());

                if (requestedFields.contains("*"))
                {
                    Set<String> finalRemainingFields = remainingFields;

                    remainingFields.addAll(fields.stream().map(SchemaField::getName).filter(o-> !finalRemainingFields.contains(o)).collect(Collectors.toList()));
                }
            }
        }

        SolrDocumentFetcher docFetcher =  rb.req.getSearcher().getDocFetcher();

        for(int i = 0; i<docList.size(); i++)
        {
            SolrDocument docBase = docList.get(i);
            int docId = (Integer) docBase.getFieldValue("luceneId");
            docFetcher.decorateDocValueFields(docBase,docId,remainingFields);
        }
    }

    @Override
    public String getDescription() {
        return "CustomGroupingSearch";
    }

    @Override
    public NamedList<Object> getStatistics(){
        NamedList stats = new SimpleOrderedMap<Object>();
        //stats.add("Custom stats totalTime(ms)",""+totalRequestTime);
        return stats;
    }

}


