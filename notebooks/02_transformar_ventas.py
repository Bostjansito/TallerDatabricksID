# Databricks notebook source
# Ejercicio 2 - Transformacion y reglas de calidad (capa silver)
from pyspark.sql import functions as F

catalogo = "workspace"
esquema = "ingenieria_datos_sebastian_perez"
tabla_bronze = f"{catalogo}.{esquema}.ventas_bronze"
tabla_silver = f"{catalogo}.{esquema}.ventas_silver"
tabla_categoria = f"{catalogo}.{esquema}.ventas_por_categoria"

# 1. Leer bronze
df_bronze = spark.table(tabla_bronze)
registros_antes = df_bronze.count()

# 2. Eliminar registros identicos duplicados
df_sin_duplicados = df_bronze.dropDuplicates()
registros_sin_duplicados = df_sin_duplicados.count()

# 3. Descartar ventas sin producto, cantidad <= 0 o precio_unitario <= 0
df_limpio = df_sin_duplicados.filter(
    F.col("producto").isNotNull()
    & (F.trim(F.col("producto")) != "")
    & (F.col("cantidad") > 0)
    & (F.col("precio_unitario") > 0)
)

# 4. Columna total_venta = cantidad * precio_unitario
df_silver = df_limpio.withColumn("total_venta", F.col("cantidad") * F.col("precio_unitario"))
registros_despues = df_silver.count()

# 5. Guardar ventas_silver (overwrite => re-ejecutar no acumula registros)
df_silver.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(tabla_silver)

# 6. Resumen por categoria
df_categoria = (spark.table(tabla_silver)
    .groupBy("categoria")
    .agg(F.sum("total_venta").alias("valor_total_vendido"),
         F.count("*").alias("numero_ventas"))
    .orderBy(F.desc("valor_total_vendido")))
df_categoria.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(tabla_categoria)

print(f"Registros en bronze (antes de limpiar):     {registros_antes}")
print(f"Registros tras eliminar duplicados:          {registros_sin_duplicados}")
print(f"Registros en silver (despues de limpiar):    {registros_despues}")
print(f"Registros descartados en total:              {registros_antes - registros_despues}")
print(f"Filas en {tabla_silver}: {spark.table(tabla_silver).count()}")
print(f"Filas en {tabla_categoria}: {spark.table(tabla_categoria).count()}")

display(spark.table(tabla_categoria))
