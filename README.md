### solrconfig.xml

Define the component:

    <searchComponent name="grouping-search" class="rubrikk.solr.CustomGroupingSearch">
    <str name="field">cat</str>
      <lst name="words">
        <str name ="word">book</str>
        <str name ="word">fish</str>
        <str name ="word">dor</str>
      </lst>
    </searchComponent>

Add the component as first-component to your `/select` handler:

    <arr name="last-components">
      <str>grouping-search</str>
    </arr>

