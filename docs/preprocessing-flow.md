You can apply functions on the dataframes of the dependencies before they are available for your node. 

Suppose you have a `nodeB` that has `nodeA` as dependency, once the `nodeA` outputs the dataframe Flypipe will check 
if there is a preprocess function set on the dependency `nodeA`, if exists, it will apply the preprocess function and 
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

1. Flypipe will apply `filter_date` on the output dataframe of `nodeA` to `nodeB`

## How dataframes are propagated throughout the nodes? 

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
  
  table e2@----> preprocessing@{ shape: das, label: "preprocess<br/>filter_date(df)" } 
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

## Disabling preprocess

### Disable all preprocess in the graph

``` python
from flypipe.mode import PreprocessMode

any_node.run(preprocess=PreprocessMode.DISABLE)
```

### Disable specific nodes preprocess dependencies

```python
from flypipe.mode import PreprocessMode

df = any_node.run(preprocess={    
    other_node: {node_depencency: PreprocessMode.DISABLE}
})
```

### Enable preprocess for all dependencies by default

Some cases you just might need to apply preprocess on all nodes dependencies, for example, you want to run a preprocess
function that filters only new data for CDC (change data capture) pipelines.

Setting environment variables `FLYPIPE_DEFAULT_DEPENDENCIES_PREPROCESS_MODULE` and `FLYPIPE_DEFAULT_DEPENDENCIES_PREPROCESS_FUNCTION`, 
will tell flypipe to use and apply the function to all dependencies of all nodes

for example if your function import looks like:

`from my_project.utils import global_preprocess`

the environment variables would look like:

``` bash
FLYPIPE_DEFAULT_DEPENDENCIES_PREPROCESS_MODULE=my_project.utils
FLYPIPE_DEFAULT_DEPENDENCIES_PREPROCESS_FUNCTION=global_preprocess
```

!!! note
    For explicit preprocess defined on node dependencies, Flypipe will prefer them over default preprocess function

## Chaining preprocessing functions

Multiples preprocess functions, i.e. `.preprocess(func1, func2...)`, can be set.

All preprocess functions will be called in the order defined:
- `.preprocess(func1, func2)` will call `func1`, then the output dataframe from `func1` will be passed to `func2`, and so on.
- `.preprocess(func2, func1)` will call `func2`, then the output dataframe from `func2` will be passed to `func1`, and so on.

**Example**:


``` py linenums="1" hl_lines="13" title="Preprocessing Syntax"
def filter_date(df):
    return df.filter(df.value >= '2025-01-02')
    
def filter_id(df):
    return df.filter(df.id >= 3)
    
@node(...)
def nodeA():
    return df
    
@node(
    dependencies=[
        nodeA.preprocess(filter_date, filter_id).alias("df") # (1)
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

1. Flypipe will apply `filter_date` and then `filter_id` on the output dataframe of `nodeA` to `nodeB`

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
  
  table e2@----> preprocessing@{ shape: das, label: "preprocess<br/>filter_date(df)" };
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
  e3@{ animate: true };
  
  preprocessedTable e4@----> preprocessing2@{ shape: das, label: "preprocess<br/>filter_id(df)" };
  e4@{ animate: true };
  
  preprocessing2 e5@----> preprocessedTable2(["
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
  e5@{ animate: true };
  
  preprocessedTable2 e6@----> nodeB(nodeB);
  e6@{ animate: true };
  
  table e7@----> nodeC(nodeC);
  e7@{ animate: true };
  
  style table fill:None,stroke-width:0px;
  style preprocessedTable fill:None,stroke-width:0px;
  style preprocessedTable2 fill:None,stroke-width:0px;
```