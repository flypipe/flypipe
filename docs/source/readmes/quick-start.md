
# Test it in databricks

```
%pip install flypipe
```

````python
from flypipe import node
from flypipe import Spark
from flypipe.converter import Schema, StringType
import pyspark.sql.functions as F


@node(type="pyspark",
    inputs=[Spark("raw.user_account", columns=["user_key", "address_state"])],
    outputs=[Schema("user_id", StringType(), primary_key=True),
             Schema("state", StringType())])
def user_state(raw_user_account):
    df = raw_user_account
    df = df.withColumnRenamed("user_key", "user_id")
    return df.withColumn("state", F.upper(F.col("address_state")))
````