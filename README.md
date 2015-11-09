# org-writer-neo
Experimental organisation writer backed by neo4j

Example use:

- Install neo4j and start it up locally


- Install org-writer-neo as follows:

```
go get -u github.com/Financial-Times/org-writer-neo
```


- Run org-writer-neo :
```
org-writer-neo
```

- Insert an organisation :

```
curl  -d'{"uuid":"d7646afe-ba78-38af-9eb7-86577b5274a4","properName":"109 West Main Street, Inc.","identifiers":[{"authority":"http://api.ft.com/system/FACTSET-EDM","identifierValue":"000BDM-E"}],"parentOrganisation":"443813be-75ed-3e7b-9ce2-7d3b8dc8fc09","hiddenLabel":"109 WEST MAIN ST INC","type":"Organisation"}' -XPUT localhost:8080/organisations/d7646afe-ba78-38af-9eb7-86577b5274a4
```

- Or insert many organisations at once (note that this does not replace collection currently):

```
curl  -d'{"uuid":"xxx"...}{"uuid":"yyy"...}' -XPUT localhost:8080/organisations/
```

- Point your browser at http://localhost:7474 and explore your organisations.
