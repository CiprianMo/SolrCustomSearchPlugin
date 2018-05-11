package rubrikk.solr;

import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.search.DocSlice;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class RubrikkGrouping {


    public class Groups
    {
        private String groupValue;

        private DocSlice doclist;

        public void setGroupValue(String groupValue){
            this.groupValue = groupValue;
        }
        public String getGroupValue(){
            return this.groupValue;
        }
        public void setDoclist(DocSlice doclist){
            this.doclist = doclist;
        }
        public DocSlice getDoclist(){
            return this.doclist;
        }
    }

    public class Domain
    {
        private int matches;

        private Map<String,Groups> groups;

        public void setMatches(int matches){
            this.matches = matches;
        }
        public int getMatches(){
            return this.matches;
        }
        public void setGroups(SimpleOrderedMap groups){
            this.groups = groups.asShallowMap();
        }
        public Map<String,Groups> getGroups(){
            return this.groups;
        }
    }

    public class Root
    {
        private Map<String,Domain> domain;

        public void setDomain(SimpleOrderedMap domain){
            this.domain = domain.asShallowMap();
        }
        public Map<String,Domain> getDomain(){
            return this.domain;
        }
    }

}
