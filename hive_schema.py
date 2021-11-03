from pyspark.sql.types import *

def hive_schema(item, layer=0):
  rel = {"ByteType": "tinyint", "ShortType": "smallint", "IntegerType": "int", "LongType": "bigint", "FloatType": "float", "DoubleType": "double", "DecimalType": "decimal(10,0)",
  "StringType": "string", "BinaryType": "binary", "BooleanType": "bool", "TimestampType": "timestamp", "DateType": "date", "LongType": "bigint", "DoubleType": "double", "DateType": "date"}
  f = ""
  if (isinstance(item, StructType)):
    for l_item in item:
      f += ", " if f != "" and layer == 0 else "," if f != "" else ""
      f += hive_schema(l_item, layer+1)
  elif (isinstance(item, MapType)):
    f += rel[str(item.keyType)] + ":" + rel[str(item.valueType)]
  elif (isinstance(item, ArrayType)):
    f += hive_schema(item.elementType, layer+1)
  elif (isinstance(item, StructField)):
    if (isinstance(item.dataType, StructType)):
      f += str(item.name) + (" struct<" if layer <= 1 else ":struct<") + hive_schema(item.dataType, layer+1) + ">"
    elif (isinstance(item.dataType, MapType)):
      f += str(item.name) + (" map<" if layer <= 1 else ":map<") + hive_schema(item.dataType, layer+1) + ">"
    elif (isinstance(item.dataType, ArrayType)):
      f += str(item.name) + (" array<" if layer <= 1 else ":array<") + hive_schema(item.dataType, layer+1) + ">"
    else:
      f += str(item.name) + (" " if layer <= 1 else ":") + rel[str(item.dataType)]
  else:
    f += rel[str(item)]
  return f
