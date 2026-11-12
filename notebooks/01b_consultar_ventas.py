# Databricks notebook source
# Ejercicio 1 - Lectura de ventas_bronze desde otro notebook
tabla = "workspace.ingenieria_datos_sebastian_perez.ventas_bronze"   # catalogo.esquema.tabla

df = spark.table(tabla)

print("Columnas y tipos de datos:")
df.printSchema()

total = df.count()
print(f"La tabla {tabla} contiene {total} registros")

categoria_elegida = "Electronica"
df_categoria = df.filter(df.categoria == categoria_elegida)
print(f"Ventas de la categoria {categoria_elegida}: {df_categoria.count()}")
display(df_categoria)
