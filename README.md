### solrconfig.xml

Define the component:

    <searchComponent name="grouping-search" class="rubrikk.solr.CustomGroupingSearch"/>

Add the component as first-component to your `/select` handler:

    <arr name="first-components">
      <str>grouping-search</str>
    </arr>

