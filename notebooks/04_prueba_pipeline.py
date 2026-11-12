# Databricks notebook source
# Ejercicio 4 - Paso 7: agregar una venta valida a ventas_bronze (SIN ejecutar 01_cargar_ventas)
esquema = "workspace.ingenieria_datos_sebastian_perez"

# Resumen ANTES de agregar la venta (ultima ejecucion del pipeline)
print("Resumen por categoria ANTES:")
display(spark.sql(f"SELECT * FROM {esquema}.pipeline_ventas_por_categoria ORDER BY categoria"))

# Venta valida nueva: 3 x 40.000 = 120.000 en la categoria Ropa
spark.sql(f"""
    INSERT INTO {esquema}.ventas_bronze
    VALUES (16, DATE'2026-10-09', 'Bufanda', 'Ropa', 3, 40000.0)
""")

print(f"Registros en ventas_bronze despues del INSERT: {spark.table(esquema + '.ventas_bronze').count()}")
display(spark.sql(f"SELECT * FROM {esquema}.ventas_bronze WHERE id_venta = 16"))

# COMMAND ----------

# Ejercicio 4 - Paso 8: resumen DESPUES de volver a ejecutar el pipeline
esquema = "workspace.ingenieria_datos_sebastian_perez"

antes = {"Alimentos": 111300.0, "Electronica": 3115000.0, "Ropa": 780000.0}   # valores capturados antes del INSERT
venta_agregada = 3 * 40000.0   # Bufanda, categoria Ropa

despues = {r["categoria"]: r["valor_total_vendido"]
           for r in spark.table(f"{esquema}.pipeline_ventas_por_categoria").collect()}

for cat in sorted(despues):
    print(f"{cat:12s} antes={antes.get(cat, 0):>12,.0f}  despues={despues[cat]:>12,.0f}  "
          f"diferencia={despues[cat] - antes.get(cat, 0):>10,.0f}")

print(f"Valor de la venta agregada: {venta_agregada:,.0f}")
print("Coincide con la diferencia en Ropa:", despues["Ropa"] - antes["Ropa"] == venta_agregada)

display(spark.sql(f"SELECT * FROM {esquema}.pipeline_ventas_por_categoria ORDER BY categoria"))
