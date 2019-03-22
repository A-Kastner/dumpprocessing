# Prerequisites

* A .csv dump of the join between categorylinks and page, e.g.:
> SELECT * FROM page JOIN categorylinks ON categorylinks.cl_from = page.page_id INTO OUTFILE 'E:/wikidumps/categorylinkspage-join.csv' FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"' LINES TERMINATED BY '\n'

* The article dump, e.g. enwiki-20190101-pages-articles-multistream.xml

* lxml:
> pip install lxml
	
* bz2file:
> pip install bz2file

# Usage:

Place the .csv dump and the unzipped .xml article dump in the /resources folder

Set the FILENAME_CSV (.csv dump), FILENAME_ARTICLES_XML (article dump), wantedCategory and maxDepth variables in script.py as needed.

Run script.py

> python script.py

Run WikiExtractor with the output from script.py as input (choose SIZE bigger than input file to receive a single output file):

> python wikiextractor\WikiExtractor.py output\[FILENAME] -o [OUTPUTPATH] -b [SIZE] --json

e.g.:

> python wikiextractor\WikiExtractor.py output\Computer_hardware-d10\Computer_hardware_articles-d10.xml.bz2 -o output/Computer_hardware-d10 -b 5G --json
