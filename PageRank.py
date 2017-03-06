#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""
Author: Akash Darak

Date: 09/21/2016
"""

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

    #lines is an RDD
    #1 means the number of partition
    
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

        # This formula is for taxatoin based calculations
        #ranks = contribs.reduceByKey(add).mapValues(lambda rank: rank * 0.85 + 0.15)
        
        #Idealized Page rank algorithm
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
	
	"""
	for (link, rank) in inp.collect():
		print("%s got appended to: %s" % (link, rank))
	
	# Joins the links file to titles
#	for (link, rank) in zip(itertools.repeat(inp), itertools.repeat(rdd)):
		#print("%s has rank : %s." % (link, rank))
	
	#print(inp.collect())
	#print(ranks.collect())
	
	
    # Collects all URL ranks and dump them to console.
    for (link, rank) in ranks.collect():
        print("%s has final pagerank: %s." % (link, rank))
	
	"""
    
