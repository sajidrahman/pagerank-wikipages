# pagerank-wikipages
Implementing PageRank Algorithm on WikiPedia pages using Hadoop Map-Reduce F/W

Driver class(hadoop.Driver) contains 6 jobs. Each of job's goals along with their input-outputs are as follow:

- Job 0 : Calculate total no. of pages

This job uses Mahout's	XmlInputFormat	which	processes	XML	files	and	extract	out	the	XML	between	two	configured	start	/	end	tags.
In case of wiki corpus, the input XML looks like this:

    <page>...</page>
    <page>
        <title>Foo</title>
        <text>Lorem ipsum dolor sit amet...</text>
    </page>
    <page>
        <title>Bar</title>
        <text>Lorem ipsum dolor sit [[Foo|amet]]...</text>
    </page>
        <page>
        <title>Baz</title>
        <text>[[Foo]] [[Bar]] not Lipsum...</text>
    </page>
    <page>...<page>
    
    The mapper class(WikiCorpusSizeCalculatorMapper) is passed a following sample text value  <page><title>Bar</title><text>Lorem ipsum dolor sit [[Foo|amet]]...</text>
   </page>, which is then gets parsed to extrat _title_ and _text body_. If its a valid title(i.e. contains no colon), mapper emits <size,1> tuple.
   
The reducer class (WikiCorpusSizeCalculatorReducer) sums up all the values and emits <size, total_page_no>.
   
- Job 1: Extract wiki pages and valid outgoing links from the pages

To extract the valid data i.e. contents of <page> ... </page> XMLInputFormat class is used as in Job 0. In first MapReduce task, the mapper extracts the title of the page which is present in the <title> ... </title> and 
all the contents of the <text> ... </text> to find all the out-links from current page. A regular expression is written to extract all the valid links from the text body. In our case there are two types of valid links, **[[out_link]]**, and **[[out_link|name]]**. 
From both links *out_link* is extracted and all the spaces in the link are replaced with underscores. 
  - All titles are emitted with (title, #) so that for every title one bucket will be created by the combiner. 
  - For each outgoing links from that page, we emit (link, title) if link doesn't refer back to the same page.
  
Now in reducer, we put all the contents of the bucket in a **Set** to keep only the unique links. 
As every page-title was emitted with a #, if # is not present in the set then the link doesn't refer to an article in given corpus (i.e. Red link). So, that link is skipped.
Now when the set has # sent as a (title, #) which will go to its specific bucket and all the other links are sent as single ou-links.
