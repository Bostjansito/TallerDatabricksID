# Ejercicio 4 - Pipeline declarativo (Lakeflow Spark Declarative Pipelines)
from pyspark import pipelines as dp
from pyspark.sql import functions as F

FUENTE = "workspace.ingenieria_datos_sebastian_perez.ventas_bronze"


@dp.materialized_view(
    name="pipeline_ventas_limpias",
    comment="Ventas sin duplicados, con producto, cantidad > 0 y precio_unitario > 0, mas total_venta",
)
def pipeline_ventas_limpias():
    return (
        spark.read.table(FUENTE)
        .dropDuplicates()
        .filter(
            F.col("producto").isNotNull()
            & (F.trim(F.col("producto")) != "")
            & (F.col("cantidad") > 0)
            & (F.col("precio_unitario") > 0)
        )
        .withColumn("total_venta", F.col("cantidad") * F.col("precio_unitario"))
    )


@dp.materialized_view(
    name="pipeline_ventas_por_categoria",
    comment="Valor total vendido por categoria (depende de pipeline_ventas_limpias)",
)
def pipeline_ventas_por_categoria():
    return (
        spark.read.table("pipeline_ventas_limpias")
        .groupBy("categoria")
        .agg(
            F.sum("total_venta").alias("valor_total_vendido"),
            F.count("*").alias("numero_ventas"),
        )
    )
