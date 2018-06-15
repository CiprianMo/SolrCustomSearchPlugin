package rubrikk.solr;

import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.ContentStream;
import org.apache.solr.core.SolrCore;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.util.RTimerTree;

import java.security.Principal;
import java.util.Map;

public class Mocks {

    public class SolrQueryRequestMock implements SolrQueryRequest {

        @Override
        public SolrParams getParams() {
            return null;
        }

        @Override
        public void setParams(SolrParams params) {

        }

        @Override
        public Iterable<ContentStream> getContentStreams() {
            return null;
        }

        @Override
        public SolrParams getOriginalParams() {
            return null;
        }

        @Override
        public Map<Object, Object> getContext() {
            return null;
        }

        @Override
        public void close() {

        }

        @Override
        public long getStartTime() {
            return 0;
        }

        @Override
        public RTimerTree getRequestTimer() {
            return null;
        }

        @Override
        public SolrIndexSearcher getSearcher() {
            return null;
        }

        @Override
        public SolrCore getCore() {
            return null;
        }

        @Override
        public IndexSchema getSchema() {
            return null;
        }

        @Override
        public void updateSchemaToLatest() {

        }

        @Override
        public String getParamString() {
            return null;
        }

        @Override
        public Map<String, Object> getJSON() {
            return null;
        }

        @Override
        public void setJSON(Map<String, Object> json) {

        }

        @Override
        public Principal getUserPrincipal() {
            return null;
        }
    }
}
