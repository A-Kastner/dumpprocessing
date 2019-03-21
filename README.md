# Requirements:

lxml:
> pip install lxml
	
bz2file:
> pip install bz2file

# Usage:

Put the sql dump (SELECT * FROM page JOIN categorylinks ON categorylinks.cl_from = page.page_id INTO OUTFILE 'E:/wikidumps/categorylinks-pages-join.csv' FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"' LINES TERMINATED BY '\n')
and article dump in /resources folder

Set the FILENAME_CSV, FILENAME_ARTICLES_XML, wantedCategory and maxDepth variables

Run script.py

> python script.py

Run WikiExtractor with the output from script.py:

> python lib\wikiextractor\WikiExtractor.py output\[FILENAME.xml.bz2] -b [SIZE] --json

e.g.
> python lib\wikiextractor\WikiExtractor.py output\Computer_hardware_articles-d5.xml.bz2 -b 5G --json
