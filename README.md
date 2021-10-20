## λouna-local
Clojars [[louna/louna-local "0.1.0-SNAPSHOT"]](https://clojars.org/louna/louna-local)  
Documentation [https://tkaryadis.github.io/louna-local](https://tkaryadis.github.io/louna-local)  

**Experimental project** 
1. Performs SPARQL like queries from data give in text files.
2. Queries use s-expressions syntax
3. Includes a query processor to answer those queries
   A large part of SPARQL language is supported.  
   joins/groupby/sortby/etc

**Other features**
1. SPARQL generation  
   Louna queries can generate SPARQL  
2. RDF transformations,rdf files are transformed to louna files  
   Louna can query RDF data,but before that it transform them to louna-data  

For a faster implementation look at [[louna/louna-spark "0.1.0-SNAPSHOT"]](https://clojars.org/louna/louna-spark)  
That is a Clojure spark library with similar to louna-local syntax,that runs on Apache Spark.
Apache Spark provides fast query processor, binary data etc.

## Examples
Most of the examples in test are solution to this book  
[Learning SPARQL](http://www.learningsparql.com/)  

Example of louna query

```text
(q (:person.city         ?person    ?city)  
   (:city.country        ?city      ?country)  
   (:country.continent   ?country   ?continent))
```

Can auto-generate

```
SELECT *
WHERE    
{
  ?person  person:city          ?city .
  ?city    city:country         ?country .  
  ?country country:continent    ?continent .  
}
```
