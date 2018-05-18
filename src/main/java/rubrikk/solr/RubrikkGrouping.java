package rubrikk.solr;

import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.queries.function.valuesource.QueryValueSource;
import org.apache.lucene.search.*;
import org.apache.lucene.search.grouping.*;
import org.apache.lucene.util.mutable.MutableValue;
import org.apache.solr.search.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;

public class RubrikkGrouping extends Grouping {
    SolrIndexSearcher searcher;
    boolean needScores;
    private Query query;
    private static final Logger Log = LoggerFactory.getLogger(RubrikkGrouping.class);
    /**
     * @param searcher
     * @param qr
     * @param cmd
     * @param cacheSecondPassSearch    Whether to cache the documents and scores from the first pass search for the second
     *                                 pass search.
     * @param maxDocsPercentageToCache The maximum number of documents in a percentage relative from maxdoc
     *                                 that is allowed in the cache. When this threshold is met,
     * @param main
     */
    public RubrikkGrouping(SolrIndexSearcher searcher, QueryResult qr, QueryCommand cmd, boolean cacheSecondPassSearch, int maxDocsPercentageToCache, boolean main) {
        super(searcher, qr, cmd, cacheSecondPassSearch, maxDocsPercentageToCache, main);
        this.searcher = searcher;

        query = QueryUtils.makeQueryable(cmd.getQuery());
        needScores = (cmd.getFlags() & SolrIndexSearcher.GET_SCORES) != 0;


    }

    @Override
    public RubrikkGrouping setLimitDefault(int li)
    {
        super.setLimitDefault(li);
        return this;
    }

    @Override
    public void execute() throws IOException {
        RubrikkGrouping.RubrikkCommandFunc funcComm = new RubrikkCommandFunc();
        funcComm.groupBy = new QueryValueSource(query,0.0f);
        super.add(funcComm);

        super.execute();
    }

    public class RubrikkCommandFunc extends Grouping.CommandFunc{
        FirstPassGroupingCollector<MutableValue> firstPass;
        TopGroupsCollector<MutableValue> secondPass;
        // If offset falls outside the number of documents a group can provide use this collector instead of secondPass
        TotalHitCountCollector fallBackCollector;
        AllGroupsCollector<MutableValue> allGroupsCollector;
        Collection<SearchGroup<MutableValue>> topGroups;
        Map context;
        public ValueSource groupBy;

        private ValueSourceGroupSelector newSelector() {
            return new ValueSourceGroupSelector(groupBy, context);
        }


        int actualGroupsToFind = 10;
        int maxDoc;

        @Override
        protected void prepare() throws IOException {
            Log.info("In prepare of RubrikkCommandFunc");
            maxDoc = searcher.maxDoc();
            Log.info("Offse= "+offset+" numGroups= "+numGroups+" maxDoc "+maxDoc);
            //actualGroupsToFind = getMax(offset, numGroups, maxDoc);
            Log.info("Act "+actualGroupsToFind);
        }

        @Override
        protected Collector createFirstPassCollector() throws IOException {
            if (actualGroupsToFind <= 0) {
                fallBackCollector = new TotalHitCountCollector();
                return fallBackCollector;
            }

            groupSort = groupSort == null ? Sort.RELEVANCE : groupSort;
            firstPass = new FirstPassGroupingCollector<>(newSelector(), searcher.weightSort(groupSort), actualGroupsToFind);
            return firstPass;
        }

        @Override
        protected Collector createSecondPassCollector() throws IOException {

            Log.info("inside CreateSecondPass, actualGroupsTofind: "+actualGroupsToFind);
            Log.info("MaxDoc = "+searcher.maxDoc());
//            if (actualGroupsToFind <= 0) {
//                allGroupsCollector = new AllGroupsCollector<>(newSelector());
//                return totalCount == TotalCount.grouped ? allGroupsCollector : null;
//            }

            Log.info("Format: "+format);

            if(firstPass == null)
            {
                Log.info("fistPass is null");
            }
            topGroups = format == Format.grouped ?
                    firstPass.getTopGroups(offset, false) :
                    firstPass.getTopGroups(0, false);

            Log.info("TopGroups: "+topGroups.size());

            if (topGroups == null) {
                if (totalCount == TotalCount.grouped) {
                    allGroupsCollector = new AllGroupsCollector<>(newSelector());
                    fallBackCollector = new TotalHitCountCollector();
                    return MultiCollector.wrap(allGroupsCollector, fallBackCollector);
                } else {
                    fallBackCollector = new TotalHitCountCollector();
                    return fallBackCollector;
                }
            }

            int groupdDocsToCollect = getMax(groupOffset, docsPerGroup, maxDoc);
            groupdDocsToCollect = Math.max(groupdDocsToCollect, 1);
            Sort withinGroupSort = this.withinGroupSort != null ? this.withinGroupSort : Sort.RELEVANCE;

            if (query instanceof RankQuery) {
                Log.info("In RankQuery: ");
                secondPass= new RubrikkReRankCollector<>(newSelector(),
                        topGroups,groupSort,withinGroupSort,groupdDocsToCollect,needScores,needScores,false);
            }
            else {
                Log.info("not in RankQuery");
                secondPass = new TopGroupsCollector<>(newSelector(),
                        topGroups, groupSort, withinGroupSort, groupdDocsToCollect, needScores, needScores, false);
            }

            return secondPass;
        }

        @Override
        protected void finish() throws IOException {

            //secondPass.getTopGroups(10);
            super.finish();
        }

        @Override
        public int getMatches() {
            return super.getMatches();
        }
    }
}

