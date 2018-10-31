# high-perf-privacy-scalaIO2018

[![Build Status](https://travis-ci.org/ogirardot/high-perf-privacy-scalaIO2018.svg?branch=master)](https://travis-ci.org/ogirardot/high-perf-privacy-scalaIO2018)

## What am I looking at ? 

It's a fully fonctionnal implementation of a Privacy Framework we designed as an illustration for the Scala.IO 2018 talk : "High performance Privacy By Design using Matryoshka and Spark" we gave at Lyon.

You have three engines here : 
* matryoshka engine: zipping data and schema together and matching semantic tags to cypher data;
* lambda engine: creates a lambda to do that "digging work once" and apply the corresponding lambda multiple times;
* codegen engine: creates an Apache Spark expression to do that work leveraging the Unsafe/Tungsten data format of Apache Spark SQL.

## Where are the slides ? 
Here you go : https://speakerdeck.com/ogirardot/high-performance-privacy-by-design-using-matryoshka-and-spark
Enjoy !
