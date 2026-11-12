# Actividad Databricks — Sebastian Perez

Workspace: Databricks Free Edition · cómputo serverless · catálogo `workspace` · esquema `ingenieria_datos_sebastian_perez`

> Las respuestas de investigación son **borradores**: la consigna pide que estén con tus propias palabras. Ajústalas a tu forma de escribir y verifica los enlaces de las fuentes.

---

## Datos de partida (16 registros)

16 ventas con `id_venta, fecha, producto, categoria, cantidad, precio_unitario`. Errores intencionales:

| Caso | Registro |
|---|---|
| Venta duplicada (idéntica) | `(12, 2026-10-06, Pan, Alimentos, 4, 3000.0)` aparece dos veces |
| Venta sin producto | `(13, 2026-10-07, NULL, Electronica, 2, 80000.0)` |
| Cantidad negativa | `(14, 2026-10-07, Monitor, Electronica, -1, 900000.0)` |

---

## 1. Almacenar y consultar datos en el catálogo

### Preguntas de investigación (borrador)

**¿Qué es Unity Catalog?**
Unity Catalog es la capa de gobierno de datos de Databricks. Organiza todos los activos de datos (tablas, vistas, volúmenes, modelos, funciones) en una jerarquía de tres niveles: `catálogo.esquema.objeto`. Centraliza los permisos (quién puede leer o escribir cada objeto), registra el linaje entre tablas y la auditoría de accesos. Así cualquier notebook, job o pipeline del workspace encuentra la misma tabla por su nombre completo.

**¿Qué diferencia hay entre un DataFrame de Spark y una tabla Delta?**
Un DataFrame es una estructura en memoria, distribuida y *lazy*: describe un conjunto de datos y las transformaciones sobre él, pero solo existe mientras dura la sesión de Spark que lo creó. Una tabla Delta es almacenamiento persistente: archivos Parquet más un log de transacciones (`_delta_log`). Ese log le da transacciones ACID, control de versiones (*time travel*), evolución de esquema y escrituras seguras en concurrencia. En resumen, el DataFrame sirve para procesar y la tabla Delta para guardar y compartir. Al hacer `df.write.saveAsTable(...)` el DataFrame se materializa como tabla Delta registrada en Unity Catalog.

Fuentes: https://docs.databricks.com/aws/en/data-governance/unity-catalog/ · https://docs.databricks.com/aws/en/delta/ · https://spark.apache.org/docs/latest/sql-programming-guide.html

### Notebook `01_cargar_ventas`

Crea el esquema y guarda `ventas_bronze` en modo `overwrite`, para que volver a ejecutarlo no acumule registros:

```python
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
ventas = [ ... 16 registros ... ]   # ver notebook en el workspace

df_ventas = spark.createDataFrame(ventas, schema)
(df_ventas.write.format("delta").mode("overwrite")
    .option("overwriteSchema", "true").saveAsTable(tabla_bronze))
```

### Notebook `01b_consultar_ventas` (lectura desde otro notebook)

```python
tabla = "workspace.ingenieria_datos_sebastian_perez.ventas_bronze"   # catalogo.esquema.tabla
df = spark.table(tabla)
df.printSchema()
print(df.count())                                      # 16
display(df.filter(df.categoria == "Electronica"))      # 6 ventas
```

**Columnas y tipos (Catalog Explorer):** `id_venta int`, `fecha date`, `producto string`, `categoria string`, `cantidad int`, `precio_unitario double`. Es una tabla Managed y su fuente de datos es Delta.

**Resultados:** la tabla tiene 16 registros, de los cuales 6 son de la categoría Electronica.

---

## 2. Transformar datos y aplicar reglas de calidad

### Notebook `02_transformar_ventas`

```python
from pyspark.sql import functions as F

df_bronze = spark.table(tabla_bronze)
df_sin_duplicados = df_bronze.dropDuplicates()
df_limpio = df_sin_duplicados.filter(
    F.col("producto").isNotNull() & (F.trim(F.col("producto")) != "")
    & (F.col("cantidad") > 0) & (F.col("precio_unitario") > 0))
df_silver = df_limpio.withColumn("total_venta", F.col("cantidad") * F.col("precio_unitario"))
df_silver.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(tabla_silver)

df_categoria = (spark.table(tabla_silver).groupBy("categoria")
    .agg(F.sum("total_venta").alias("valor_total_vendido"), F.count("*").alias("numero_ventas")))
df_categoria.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(tabla_categoria)
```

### Resultados

| Etapa | Registros |
|---|---|
| `ventas_bronze` (antes de limpiar) | 16 |
| Después de `dropDuplicates()` | 15 |
| `ventas_silver` (después de limpiar) | **13** |
| Descartados en total | 3 (duplicado, sin producto, cantidad negativa) |

**`ventas_por_categoria`:**

| categoria | valor_total_vendido | numero_ventas |
|---|---:|---:|
| Electronica | 3.115.000 | 4 |
| Ropa | 780.000 | 5 |
| Alimentos | 111.300 | 4 |

**Idempotencia:** al ejecutar el notebook por segunda vez, `ventas_silver` sigue con 13 filas y `ventas_por_categoria` con 3. Como ambas tablas se escriben con `mode("overwrite")`, cada ejecución reemplaza el contenido en vez de añadirlo.

---

## 3. Automatizar el proceso mediante un job

### Preguntas de investigación (borrador)

**¿Qué es un job y para qué sirve?**
Un job (Lakeflow Jobs) es la unidad de orquestación de Databricks. Agrupa uno o más pasos de trabajo y permite ejecutarlos sin intervención manual: a demanda, con un horario (*schedule*) o por eventos como la llegada de archivos. Además gestiona el cómputo, los reintentos, las notificaciones y el historial de ejecuciones.

**¿Qué es una tarea y en qué se diferencia de un job?**
Una tarea es un paso concreto dentro de un job: ejecutar un notebook, un script, una consulta SQL, un pipeline, etc. El job es el contenedor que define qué tareas hay, en qué orden se ejecutan, con qué cómputo y cuándo. Un job tiene una o varias tareas, y cada tarea pertenece a un job.

**¿Qué significa establecer dependencias entre tareas?**
Significa indicar que una tarea solo empieza cuando otra ha terminado, y bajo qué condición (por ejemplo, *All succeeded*). Así se forma un grafo dirigido (DAG) que fija el orden de ejecución. Si la tarea previa falla, las dependientes no se ejecutan y quedan en estado *Upstream failed*.

**¿Qué información del historial permite identificar un fallo?**
- El estado de cada ejecución y de cada tarea (*Succeeded*, *Failed*, *Upstream failed*).
- La duración y el número de intentos o reintentos.
- El código y el mensaje de error (por ejemplo `UNRESOLVED_COLUMN`, SQLSTATE 42703).
- La salida del notebook con la celda y la línea que fallaron.
- Quién o qué lanzó la ejecución (*Manually*, *By retry scheduler*), las horas de inicio y fin, y los logs y métricas.

Fuentes: https://docs.databricks.com/aws/en/jobs/ · https://docs.databricks.com/aws/en/jobs/run-if · https://docs.databricks.com/aws/en/jobs/monitor

### Configuración: `job_ventas_sebastian_perez`

- Tarea `cargar_ventas`: ejecuta `01_cargar_ventas` en serverless.
- Tarea `transformar_ventas`: ejecuta `02_transformar_ventas`. Depende de `cargar_ventas` con la condición *Run if: All succeeded*.

### Ejecuciones

| Run ID | Resultado | Detalle |
|---|---|---|
| 15175291465414 | Succeeded (57 s) | cargar_ventas 38 s, transformar_ventas 17 s |
| 799846210346102 | **Failed** (31 s) | cargar_ventas *Failed* (2 intentos), transformar_ventas *Upstream failed* (0 s) |
| 262147302909660 | Succeeded (33 s) | ejecución final tras corregir el error |

### Explicación del error controlado

En `01_cargar_ventas`, justo después de crear el esquema, añadí esta línea:

```python
spark.table(tabla_bronze).select("columna_inexistente").count()
```

- **Qué pasó con la segunda tarea:** `transformar_ventas` no llegó a ejecutarse (0 s). Quedó en *Upstream failed* porque su dependencia exige que `cargar_ventas` termine con éxito.
- **Cómo identifiqué la causa:** en el historial del job, la ejecución aparece como *Failed*. Al abrir la tarea `cargar_ventas`, la salida del notebook muestra `[UNRESOLVED_COLUMN.WITH_SUGGESTION] A column ... with name 'columna_inexistente' cannot be resolved. Did you mean one of the following? [cantidad, id_venta, precio_unitario, categoria, fecha]. SQLSTATE: 42703`, señalando la línea 11 de la celda. También se ve que el *retry scheduler* reintentó la tarea una vez (2 intentos) antes de marcarla como fallida.
- **Corrección:** eliminé la línea y volví a ejecutar el job. Ambas tareas terminaron en *Succeeded*.

---

## 4. Construir un pipeline declarativo

### Preguntas de investigación (borrador)

**¿Qué es un pipeline de datos y cuáles son sus etapas habituales?**
Es una secuencia automatizada de pasos que mueve datos desde sus fuentes hasta un destino donde se pueden usar. Las etapas habituales son:
1. Ingesta o extracción.
2. Limpieza y validación de calidad.
3. Transformación o enriquecimiento.
4. Agregación.
5. Carga o publicación para su consumo en reportes, dashboards o modelos.

En la arquitectura medallion estas etapas corresponden a las capas bronze (crudo), silver (limpio) y gold (agregado).

**¿Qué significa que un pipeline sea declarativo?**
Significa que se define **qué** conjuntos de datos se quieren obtener y con qué lógica (por ejemplo, una vista materializada definida por una consulta), no **cómo** ni **en qué orden** ejecutar cada paso. El motor (Lakeflow Spark Declarative Pipelines) analiza las referencias entre datasets, construye el grafo de dependencias y decide el orden de ejecución. También gestiona el cómputo, los reintentos y si cada actualización puede ser incremental o requiere recálculo completo.

**¿En qué se diferencian un job que ejecuta notebooks y un pipeline declarativo?**
- En un **job** el orden es imperativo: el usuario define las tareas y sus dependencias a mano, y cada notebook decide cómo leer y escribir (por ejemplo, `overwrite`).
- En un **pipeline declarativo** se definen datasets (tablas, vistas materializadas, streaming tables). Databricks infiere las dependencias a partir del código, mantiene los resultados actualizados, puede refrescarlos de forma incremental y muestra el grafo de linaje automáticamente.

Un job orquesta **pasos** y un pipeline declara **resultados**. Pueden combinarse: un job puede tener una tarea que ejecute un pipeline.

Fuentes: https://docs.databricks.com/aws/en/ldp/ · https://docs.databricks.com/aws/en/ldp/concepts · https://www.databricks.com/glossary/medallion-architecture

### Pipeline `pipeline_ventas_sebastian_perez`

Es un pipeline ETL serverless que publica en `workspace.ingenieria_datos_sebastian_perez`. Archivo `transformations/my_transformation.py`:

```python
from pyspark import pipelines as dp
from pyspark.sql import functions as F

FUENTE = "workspace.ingenieria_datos_sebastian_perez.ventas_bronze"

@dp.materialized_view(name="pipeline_ventas_limpias")
def pipeline_ventas_limpias():
    return (spark.read.table(FUENTE)
        .dropDuplicates()
        .filter(F.col("producto").isNotNull() & (F.trim(F.col("producto")) != "")
                & (F.col("cantidad") > 0) & (F.col("precio_unitario") > 0))
        .withColumn("total_venta", F.col("cantidad") * F.col("precio_unitario")))

@dp.materialized_view(name="pipeline_ventas_por_categoria")
def pipeline_ventas_por_categoria():
    return (spark.read.table("pipeline_ventas_limpias")
        .groupBy("categoria")
        .agg(F.sum("total_venta").alias("valor_total_vendido"),
             F.count("*").alias("numero_ventas")))
```

**Grafo:** `ventas_bronze` (fuente Delta) → `pipeline_ventas_limpias` → `pipeline_ventas_por_categoria`. La dependencia se infiere sola porque la segunda vista lee a la primera.

### Prueba: agregar una venta sin ejecutar el notebook de carga

En el notebook `04_prueba_pipeline` inserté directamente en bronze una venta válida (3 × 40.000 = **120.000**):

```sql
INSERT INTO workspace.ingenieria_datos_sebastian_perez.ventas_bronze
VALUES (16, DATE'2026-10-09', 'Bufanda', 'Ropa', 3, 40000.0)
```

Con esto `ventas_bronze` pasa de 16 a 17 registros. Después volví a ejecutar solo el pipeline.

| | 1.ª ejecución | 2.ª ejecución |
|---|---|---|
| `pipeline_ventas_limpias` | 13 filas · Full recompute | 14 filas · **Incremental** |
| `pipeline_ventas_por_categoria` | 3 filas · Full recompute | 3 filas · **Incremental** |

| categoria | antes | después | diferencia |
|---|---:|---:|---:|
| Alimentos | 111.300 | 111.300 | 0 |
| Electronica | 3.115.000 | 3.115.000 | 0 |
| Ropa | 780.000 | **900.000** | **120.000** |

**Conclusión:** el resumen sí coincide con la venta agregada. Solo cambió Ropa, que subió exactamente 120.000 (de 5 a 6 ventas), y las demás categorías quedaron igual. El pipeline detectó el cambio en la fuente y actualizó ambas vistas materializadas de forma incremental, respetando el orden del grafo, sin volver a ejecutar el notebook de carga.

> Nota: si vuelves a ejecutar `01_cargar_ventas` o el job, `ventas_bronze` se sobrescribe con los 16 registros originales y la venta 16 desaparece.
