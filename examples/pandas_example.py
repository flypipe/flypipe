
import pandas

df1 = pandas.DataFrame(data={"aa": [1]})
df2 = pandas.DataFrame(data={"ab": [1]})
print(type(df1.dtypes))
print(type(df2.dtypes))

print(df1.dtypes.equals(df2.dtypes))
