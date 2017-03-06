
from __future__ import print_function

import re
import sys
import itertools

from operator import add
from pyspark import SparkContext


def funcParse(urls):
	"""Parses a urls pair string into urls pair."""
	parts = urls.split(': ') 
	return parts[0], parts[1]
    

def funcContri(urls, rank):
    	"""Calculates URL contributions to the rank of other URLs."""
    	urls = urls.split(' ')
    	num_urls = len(urls)
    	for url in urls:
        	yield (url, rank / num_urls)

if __name__ == "__main__":
	if len(sys.argv) != 3:
		print("Please check the Usage: pagerank <file> <iterations>", file=sys.stderr)
        exit(-1)
	
    # Initializing the spark context.
    sc = SparkContext(appName="PythonPageRank")

    #1 means the number of partitions
    rddline = sc.textFile(sys.argv[1], 1)
	
    # Separate each element in the list into a tuple
    outlinks = rddline.map(lambda urls: funcParse(urls))
	
    # Loads all URLs with other URL(s) link to from input file and initialize ranks of them to one.
    ranks = outlinks.map(lambda url_neighbors: (url_neighbors[0], 1.0))
	
    	#mylist = list(set([ranks]))
    	#mylist.append
	#count = int(sys.argv[2])
	
    # Calculates and updates URL ranks continuously using PageRank algorithm.
    for iteration in range(int(sys.argv[2])):
        contribs = outlinks.join(ranks).flatMap(
            lambda url_urls_rank: funcContri(url_urls_rank[1][0], url_urls_rank[1][1]))

        #Taxation based calculations
        #ranks = contribs.reduceByKey(add).mapValues(lambda rank: rank * 0.85 + 0.15)
        
        #Idealized based calculations
        ranks = contribs.reduceByKey(add).mapValues(lambda rank: rank)
	
	#Add rankings to Titles.txt
	
	titles = sc.textFile('/Lab1/titles-sorted.txt', 1)
	index = sc.parallelize(range(1, 5716809), 1)
	#inp = sc.textFile('/Lab1/titles-sorted.txt').zipWithIndex()
	tmpf = index.zip(titles)
	
	filemap = tmpf.map(lambda (index, titles): (str(index), titles))
	finale = filemap.join(ranks).map(lambda (index, ran): ran).sortBy(lambda a: a[1], False)
		
	finale.saveAsTextFile("outputfile")
	sc.stop()
    
