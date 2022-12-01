## Recommendations

- Make use of python *imports*, save your nodes in python files (not notebooks)
- Whenever possible 1 python file should represent only 1 node
- Each py file should have its own test files associated to it (i.e. `node_1.py` and `node_1_test.py`)

## Databricks

- Make use of [ Databricks repos](https://docs.databricks.com/repos/index.html)
- save your flypipe nodes inside a package in the *root*, it will facilitate python imports

### `project folder structure` (example):

```
|--project
|  |--notebooks
|  |  |--streams
|  |  |  |--your streams notebooks here
|  |  |--dlts
|  |  |  |--your dlts notebooks here
|  |  |--models
|  |  |  |--your models notebooks here
|  |--lib
|  |  |--api
|  |  |  |--your api files here
|  |  |--pipeline
|  |  |  |--data
|  |  |  |  |--your data nodes py files here
|  |  |  |--feature
|  |  |  |  |--your feature nodes py files here
|  |  |  |--model
|  |  |  |  |--demo
|  |  |  |  |  |--train
|  |  |  |  |  |  |--split.py
|  |  |  |  |  |  |--split_test.py
|  |  |  |  |  |  |--fit_scale.py
|  |  |  |  |  |  |--fit_scale_test.py
|  |  |  |  |  |  |--train_svm_model.py
|  |  |  |  |  |  |--train_svm_model_test.py
|  |  |  |  |  |  |--evaluate.py
|  |  |  |  |  |  |--evaluate_test.py
|  |  |  |  |  |--predict       
|  |  |  |  |  |  |--scale.py
|  |  |  |  |  |  |--scale_test.py
|  |  |  |  |  |  |--predict.py
|  |  |  |  |  |  |--predict_test.py
|  |  |  |  |  |--data.py
|  |  |  |  |  |--data_test.py
|  |  |  |  |  |--graph.py
|  |  |  |  |  |--graph_test.py
```






