package rubrikk.solr;

import org.apache.lucene.search.Sort;
import org.apache.lucene.search.grouping.GroupSelector;
import org.apache.lucene.search.grouping.TopGroups;
import org.apache.lucene.search.grouping.TopGroupsCollector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;

public class RubrikkReRankCollector<T> extends TopGroupsCollector {


    private static final Logger Log = LoggerFactory.getLogger(RubrikkReRankCollector.class);

    public RubrikkReRankCollector(GroupSelector groupSelector, Collection collection, Sort groupSort, Sort withinGroupSort, int maxDocsPerGroup, boolean getScores, boolean getMaxScores, boolean fillSortFields) {
        super(groupSelector, collection, groupSort, withinGroupSort, maxDocsPerGroup, getScores, getMaxScores, fillSortFields);
    }

    public TopGroups getTopGroups(int withinGroupOffset) {
        Log.info("in RubrikkReRankCollector, withinGroupOffset: "+withinGroupOffset);
        return super.getTopGroups(withinGroupOffset);
    }
}
