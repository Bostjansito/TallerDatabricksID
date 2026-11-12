# Taller Databricks — Ingeniería de Datos 2026-2

Sebastian Perez · Universidad EIA

Actividad realizada en Databricks Free Edition con cómputo serverless, catálogo `workspace` y esquema `ingenieria_datos_sebastian_perez`.

## Contenido

| Ruta | Descripción |
|---|---|
| `docs/Actividad Databricks.pdf` | Enunciado de la actividad |
| `docs/Actividad_Databricks_Sebastian_Perez.docx` | Documento de entrega con respuestas, código, resultados y capturas |
| `docs/entrega_databricks.md` | Versión en Markdown de la entrega |
| `docs/capturas/` | Capturas de evidencia de cada ejercicio |
| `notebooks/` | Código de los notebooks y del pipeline |

## Ejercicios

1. **Almacenar y consultar datos en el catálogo:** `01_cargar_ventas.py` y `01b_consultar_ventas.py`. Crean la tabla Delta `ventas_bronze` con 16 registros.
2. **Transformar datos y aplicar reglas de calidad:** `02_transformar_ventas.py`. Genera `ventas_silver` (13 registros) y `ventas_por_categoria`.
3. **Automatizar el proceso mediante un job:** `job_ventas_sebastian_perez`, con dos tareas dependientes (configurado en la interfaz de Databricks).
4. **Construir un pipeline declarativo:** `pipeline_my_transformation.py` (Lakeflow Spark Declarative Pipelines) y `04_prueba_pipeline.py`, que verifica la actualización incremental.
