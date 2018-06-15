### solrconfig.xml

Define the component:

    <searchComponent name="grouping-search" class="rubrikk.solr.CustomGroupingSearch">
          <lst name="placementScore">
            <float name ="score">1</float>
            <float name ="score">0.89</float>
            <float name ="score">0.82</float>
            <float name ="score">0.78</float>
            <float name ="score">0.76</float>
            <float name ="score">0.75</float>
            <float name ="score">0.74</float>
            <float name ="score">0.73</float>
            <float name ="score">0.72</float>
            <float name ="score">0.71</float>
            <float name ="score">0.7</float>
            <float name ="score">0.69</float>
            <float name ="score">0.64</float>
            <float name ="score">0.55</float>
            <float name ="score">0.42</float>
            <float name ="score">0.28</float>
            <float name ="score">0.15</float>
            <float name ="score">0.1</float>
            <float name ="score">0.08</float>
            <float name ="score">0.06</float>
            <float name ="score">0.05</float>
            <float name ="score">0.04</float>
            <float name ="score">0.03</float>
            <float name ="score">0.02</float>
            <float name ="score">0.01</float>
          </lst>
        </searchComponent>

Add the component as first-component to your `/select` handler:

    <arr name="last-components">
      <str>grouping-search</str>
    </arr>

