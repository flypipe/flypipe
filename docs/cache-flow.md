The following flow diagrams demonstrate how Flypipe interacts with a node to handle its cache management.

## Case 1: `node.cache is None`

``` mermaid
sequenceDiagram
  autonumber
  Flypipe->>node: node.cache is None?
  node->>Flypipe: No
  Flypipe->>node: run node
  node->>Flypipe: retrieve dataframe 
```

## Case 2: `node.cache is not None` and cache exists

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


## Case 2: `node.cache is not None` and cache does NOT exist
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