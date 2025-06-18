You can apply functions on the dataframes of the dependencies before they are available for your node. 

Suppose you have a `nodeB` that has `nodeA` as dependency, once the `nodeA` outputs the dataframe Flypipe will check 
if there is a preprocess function set on the dependency nodeA, if exists, it will apply the preprocess function and 
pass the result on to `nodeB`.

Here is the syntax to activate preprocess:

``` py linenums="1" hl_lines="10" title="Preprocessing Syntax"
def filter_date(df):
    return df.filter(df.value >= '2025-01-03')
    
@node(...)
def nodeA():
    return df
    
@node(
    dependencies=[
        nodeA.preprocess(filter_date).alias("df") # (1)
    ]
)
def nodeB(df):
    return df

@node(
    dependencies=[
        nodeA.alias("df")
    ]
)
def nodeC(df):
    return df    
```

1. You can apply a function `filter_date` on the output dataframe of `nodeA` to `nodeB` by defining it in `.preprocess`

### Here is is how the dataframe will be propagated throughout the nodes 

``` mermaid
---
config:
  flowchart:
    htmlLabels: true
---
flowchart LR
  nodeA(nodeA) e1@----> table(["
    <table style='border-color:gray; border-style:solid; border-width:1px; font-size:10px;'>
        <thead>
            <tr>
                <th>id</th>
                <th>date</th>
            </tr>
        </thead>
        <tbody>
            <tr>
                <td>1</td>
                <td>2025-01-01</td>
            </tr>
            <tr>
                <td>2</td>
                <td>2025-01-02</td>
            </tr>
            <tr>
                <td>3</td>
                <td>2025-01-03</td>
            </tr>      
        </tbody>
    </table>
  "]);
  e1@{ animate: true };
  
  table e2@----> preprocessing@{ shape: das, label: "filter_date(df)" } 
  e2@{ animate: true };
  
  preprocessing e3@----> preprocessedTable(["
     <table style='border-color:gray; border-style:solid; border-width:1px; font-size:10px;'>
        <thead>
            <tr>
                <th>id</th>
                <th>date</th>
            </tr>
        </thead>
        <tbody>
            <tr>
                <td>3</td>
                <td>2025-01-03</td>
            </tr>
        </tbody>
    </table>
  "]);
  e3@{ animate: true };
  
  preprocessedTable e4@----> nodeB(nodeB);
  e4@{ animate: true };
  
  table e5@----> nodeC(nodeC);
  e5@{ animate: true };
  
  style table fill:None,stroke-width:0px;
  style preprocessedTable fill:None,stroke-width:0px;
```