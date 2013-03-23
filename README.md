README
======

Circular is mainly a NXX-to-Elasticsearch with CLI parser atteched to it.
It's also an exercise in writing a Apache Camel Component.

To start the backend for [instanttrips](https://github.com/miku/instanttrips):

1. Copy elasticsearch-s.yml and adjust `path.data` and `path.logs` directories. 
2. Start elasticsearch 

        $ elasticsearch -Xmx4g -Xms4g -Des.config=elasticsearch-s.yml -f

To import turtle files into elasticsearch, run

1. Build the circular importer:

        $ cd circular
        $ mvn clean package

2. Run the executable:

        $ target/circular --help


    