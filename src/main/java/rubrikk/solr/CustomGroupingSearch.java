package rubrikk.solr;

import org.apache.solr.handler.component.ResponseBuilder;
import org.apache.solr.handler.component.SearchComponent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class CustomGroupingSearch extends SearchComponent {

    private static final Logger Log = LoggerFactory.getLogger(CustomGroupingSearch.class);

    public void prepare(ResponseBuilder responseBuilder) throws IOException {
        Log.info("Prepare method");
    }

    public void process(ResponseBuilder responseBuilder) throws IOException {
        Log.info("Process method");
    }

    public String getDescription() {
        return "CustomGroupingSearch";
    }
}
