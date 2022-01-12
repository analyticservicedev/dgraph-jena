
Schema 
```
curl 'http://127.0.0.1:8080/alter'   -H 'Connection: keep-alive'   -H 'Pragma: no-cache'   -H 'Content-Type: text/plain;charset=UTF-8'  -H 'Accept: */*'     --data-raw '<xid>: string @index(hash) .'   --compressed
```

##setup
### local dgraph server in docker
`docker-compose -f docker-compose.yml up` 
### setup schema
1. go into the container

    `docker exec -it dgraph_alpha_1 bash`
2. then alter schema

   `curl 'http://127.0.0.1:8080/alter'   -H 'Connection: keep-alive'   -H 'Pragma: no-cache'   -H 'Content-Type: text/plain;charset=UTF-8'  -H 'Accept: */*'     --data-raw '<xid>: string @index(hash) .'   --compressed`    
3. mvn clean install
4. start `aas.dgraph.jena.App` 
5. load data via s-put
   `cd jena-bin && ./s-put http://localhost:6384/dataset default data.trig`
6. query data via s-get
    `./s-get http://localhost:6384/ds default `