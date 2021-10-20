## λouna-local
Clojars [[louna/louna-local "0.1.0-SNAPSHOT"]](https://clojars.org/louna/louna-local)  
Documentation [https://tkaryadis.github.io/louna-local](https://tkaryadis.github.io/louna-local)  

**Project is for educational purposes only,it contains**  
1. Query language similar to SPARQL but with Lispy syntax  
2. SPARQL generation  
   Louna queries can generate SPARQL  
3. Query processor  
   A large part of SPARQL language is supported.  
   joins/groupby/sortby/etc  
4. RDF transformations,rdf files are transformed to louna files  
   Louna can query RDF data,but before that it transform them to louna-data  

For a faster implementation look at [[louna/louna-spark "0.1.0-SNAPSHOT"]](https://clojars.org/louna/louna-spark)  
That is a Clojure spark library with similar to louna-local syntax,that runs on Apache Spark so its very fast.   

## Examples
Most of the examples in test are solution to this book  
[Learning SPARQL](http://www.learningsparql.com/)  
