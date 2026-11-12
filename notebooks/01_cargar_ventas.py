# Databricks notebook source
# Ejercicio 1 - Carga de ventas (capa bronze)
from datetime import date
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType, DoubleType

catalogo = "workspace"
esquema = "ingenieria_datos_sebastian_perez"
tabla_bronze = f"{catalogo}.{esquema}.ventas_bronze"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalogo}.{esquema}")

schema = StructType([
    StructField("id_venta", IntegerType(), True),
    StructField("fecha", DateType(), True),
    StructField("producto", StringType(), True),
    StructField("categoria", StringType(), True),
    StructField("cantidad", IntegerType(), True),
    StructField("precio_unitario", DoubleType(), True),
])

ventas = [
    (1,  date(2026, 10, 1), "Laptop",    "Electronica", 1,  2500000.0),
    (2,  date(2026, 10, 1), "Mouse",     "Electronica", 3,  45000.0),
    (3,  date(2026, 10, 2), "Camiseta",  "Ropa",        2,  60000.0),
    (4,  date(2026, 10, 2), "Pantalon",  "Ropa",        1,  120000.0),
    (5,  date(2026, 10, 3), "Arroz",     "Alimentos",   5,  4500.0),
    (6,  date(2026, 10, 3), "Leche",     "Alimentos",   6,  3800.0),
    (7,  date(2026, 10, 4), "Audifonos", "Electronica", 2,  150000.0),
    (8,  date(2026, 10, 4), "Chaqueta",  "Ropa",        1,  250000.0),
    (9,  date(2026, 10, 5), "Cafe",      "Alimentos",   3,  18000.0),
    (10, date(2026, 10, 5), "Teclado",   "Electronica", 1,  180000.0),
    (11, date(2026, 10, 6), "Zapatos",   "Ropa",        1,  220000.0),
    (12, date(2026, 10, 6), "Pan",       "Alimentos",   4,  3000.0),
    (12, date(2026, 10, 6), "Pan",       "Alimentos",   4,  3000.0),     # venta duplicada
    (13, date(2026, 10, 7), None,        "Electronica", 2,  80000.0),    # venta sin producto
    (14, date(2026, 10, 7), "Monitor",   "Electronica", -1, 900000.0),   # cantidad negativa
    (15, date(2026, 10, 8), "Gorra",     "Ropa",        2,  35000.0),
]

df_ventas = spark.createDataFrame(ventas, schema)

# overwrite: cada ejecucion reemplaza la tabla en lugar de acumular registros
(df_ventas.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(tabla_bronze))

print(f"Tabla {tabla_bronze} creada con {df_ventas.count()} registros")
display(spark.table(tabla_bronze))
