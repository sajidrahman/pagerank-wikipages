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
    
 The mapper class(WikiCorpusSizeCalculatorMapper) is passed a following sample text value  <page><title>Bar</title><text>Lorem ipsum dolor sit [[Foo|amet]]...</text></page>, which is then gets parsed to extract _title_ and _text body_. If its a valid title(i.e. contains no colon), mapper emits (size,1) tuple.
   
The reducer class (WikiCorpusSizeCalculatorReducer) sums up all the values and emits (size, total_page_no).
   
- Job 1: Extract wiki pages and valid outgoing links from the pages

To extract the valid data i.e. contents of <page> ... </page> XMLInputFormat class is used as in Job 0. In first MapReduce task, the mapper extracts the title of the page which is present in the <title> ... </title> and 
all the contents of the <text> ... </text> to find all the out-links from current page. A regular expression is written to extract all the valid links from the text body. In our case there are two types of valid links, **[[out_link]]**, and **[[out_link|name]]**. 
From both links *out_link* is extracted and all the spaces in the link are replaced with underscores. 
  - All titles are emitted with (title, #) so that for every title one bucket will be created by the combiner. 
  - For each outgoing links from that page, we emit (link, title) if link doesn't refer back to the same page.
  
Now in reducer, we put all the contents of the bucket in a **Set** to keep only the unique links. 
As every page-title was emitted with a #, if # is not present in the set then the link doesn't refer to an article in given corpus (i.e. Red link). So, that link is skipped.
Now when the set contains a #, it means that the link has a valid article in the corpus. Now the reducer emits (link, #) along with (outlink, source_page_link).

- Job 2: Generate adjacency graph

The mapper takes input from the output of Job-1, splits the value based on delimiter (tab) and emits (linkA, linkB).
Reducer gets input as {linkA, [B1,B2,B3...Bn]} where link A is the source page and the list contains all the outgoing links from that page. Reducer calculates initial page rank (1/N) and emits (linkA, 1/N, list-of-outgoing-links).

- Job 3: Page Rank Calculation (Iterative map-reduce)
For 8 iterations, we split the values into 3 parts: title, rank and out-links. Now, count all the out-links for that page title and emil each out-going link with its source page rank and no. of total out-links. 
That is (out-linkA, source-pagerank, total-outlinks-of-sourcepage). The mapper also emits the page titel along with its outgoing links.

Now in Reducer for each link A, we add all the rank votes from all the incoming links to that page and count the page rank of that page. Formula for the page rank is,
PR(p1) = d/N + (1-d)*(PR(p2)/L(p2) + PR(p3)/L(p3) + ...) where
d = damping factor
PR(p1) = page rank of page p1
N = total number of pages
L(p2) = total number of ou-links on page p2
Once we are done with the page rank calculation, we re-construct the source-page with its outgoing links along with newly calculated page rank, i.e. emit (title new-rank out-links).

  - Job 3.1: Renormalization job
  
    This job takes the output from job 3 as input. The setup method accumulates all the ranks and calculates normalization         factor, c = 1/summation-of-pageranks. The mapper multiplies c with the existing page rank and emits (title normalized-rank     out-links).
  
- Job 4: Rank the pages in descending order of pagerank values