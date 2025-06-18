## Case 1: node has no cache

``` mermaid
sequenceDiagram
  autonumber
  Flypipe->>node: node.cache is None?
  node->>Flypipe: No
  Flypipe->>node: run node
  node->>Flypipe: retrieve dataframe 
```

## Case 2: node has cache, but it has been saved

``` mermaid
sequenceDiagram
  autonumber
  Flypipe->>node: node.cache is None?
  node->>Flypipe: Yes
  Flypipe->>node: Cache exists (run method `exists`)?
  node->>Flypipe: Yes
  Flypipe->>node: Read cache (run method `read`)
  node->>Flypipe: retrieve dataframe 
```


## Case 2: node has cache and has NOT been saved
``` mermaid
sequenceDiagram
  autonumber
  Flypipe->>node: node.cache is None?
  node->>Flypipe: Yes
  Flypipe->>node: Cache exists (run method `exists`)?
  node->>Flypipe: No
  Flypipe->>node: run node
  node->>Flypipe: retrieve dataframe
  Flypipe->>node: Save cache (run method `write`)?
  
```