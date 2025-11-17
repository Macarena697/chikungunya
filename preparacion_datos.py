# Databricks notebook source
# ==============================================================================
# OBJETIVO: Cargar datos brutos de casos, limpiarlos, enriquecerlos con
#           información geográfica y guardar una tabla base limpia.
# ==============================================================================

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, expr
from functools import reduce

# COMMAND ----------

spark = SparkSession.builder.appName("workspace").getOrCreate()

# COMMAND ----------

df_municipios = spark.table("workspace.default.municipios")
df_estados = spark.table("workspace.default.estados")

df_geo_maestra = df_municipios.join(
    df_estados,
    df_municipios.codigo_uf == df_estados.codigo_uf,
    "left"
).select(
    df_municipios.codigo_ibge,
    df_municipios.nome.alias("nome_municipio"),
    df_municipios.latitude.alias("lat_municipio"),
    df_municipios.longitude.alias("lon_municipio"),
    df_estados.uf.alias("uf"),
    df_estados.nome.alias("nome_estado"),
    df_estados.regiao
)

df_geo_preparado = df_geo_maestra.withColumn(
    "municipio_id_6_digitos",
    expr("substring(cast(codigo_ibge as string), 1, 6)")
).dropDuplicates(["municipio_id_6_digitos"])

# COMMAND ----------

# MAGIC %md
# MAGIC UNIR OS DATASET

# COMMAND ----------

# Cargar todos os datsets
df_25 = spark.table("workspace.default.chikbr_25")
df_24 = spark.table("workspace.default.chikbr_24")
df_23 = spark.table("workspace.default.chikbr_23")
df_22 = spark.table("workspace.default.chikbr_22")

dataframes_a_unir = [df_25, df_24, df_23, df_22]

# Inicializa un DataFrame unificado con el primer elemento de la lista
df_all = dataframes_a_unir[0]

# Recorre el resto de la lista y une cada DataFrame
for df_siguiente in dataframes_a_unir[1:]:
    df_all = df_all.unionByName(df_siguiente)

# COMMAND ----------

# MAGIC %md
# MAGIC LImpeza de daddos 

# COMMAND ----------

df_limpio = df_all.select(
    # Seleccionamos y renombramos TODAS las columnas que temos interesse
    col("DT_NOTIFIC").alias("fecha_notificacion"),
    col("NU_ANO").alias("ano_notificacion"),
    col("ID_MUNICIP").alias("municipio_id_6_digitos"),
    col("ANO_NASC").alias("ano_nacimiento"),
    col("CS_SEXO").alias("sexo"),
    col("CS_GESTANT").alias("gestante"),
    col("DT_INVEST").alias("fecha_investigacion"),
    col("DT_CHIK_S1").alias("fecha_chik_s1"),
    col("DT_CHIK_S2").alias("fecha_chik_s2"),
    col("RES_CHIKS1").alias("resultado_chik_1_id"),
    col("RES_CHIKS2").alias("resultado_chik_2_id"),
    col("DT_SORO").alias("fecha_serologia"),
    col("RESUL_SORO").alias("resultado_serologia_id"),
    col("HOSPITALIZ").alias("fue_hospitalizado"),
    col("COUFINF").alias("id_uf_infeccion"),
    col("CLASSI_FIN").alias("clasificacion_final_id"),
    col("CRITERIO").alias("criterio_confirmacion_id"),
    col("DOENCA_TRA").alias("es_enfermedad_laboral"),
    col("CLINC_CHIK").alias("sospecha_clinica_chik"),
    col("EVOLUCAO").alias("evolucion_caso_id")
).withColumn(
    "edad",
    expr("try_cast(ano_notificacion as integer) - try_cast(ano_nacimiento as integer)")
).withColumn(
    "fecha_notificacion", expr("try_to_date(fecha_notificacion, 'yyyyMMdd')")
).withColumn(
    "fecha_investigacion", expr("try_to_date(fecha_investigacion, 'yyyyMMdd')")
).withColumn(
    "fecha_chik_s1", expr("try_to_date(fecha_chik_s1, 'yyyyMMdd')")
).withColumn(
    "fecha_chik_s2", expr("try_to_date(fecha_chik_s2, 'yyyyMMdd')")
).withColumn(
    "fecha_serologia", expr("try_to_date(fecha_serologia, 'yyyyMMdd')")
).withColumn(
    "sexo", when(col("sexo") == 'M', "Masculino").when(col("sexo") == 'F', "Femenino").otherwise("Ignorado")
).withColumn(
    "evolucion_caso",
    when(col("evolucion_caso_id") == 1, "Cura")
    .when(col("evolucion_caso_id") == 2, "Óbito pelo agravo")
    .when(col("evolucion_caso_id") == 3, "Óbito por outras causas")
    .when(col("evolucion_caso_id") == 4, "Óbito em investigação")
    .when(col("evolucion_caso_id") == 9, "Ignorado")
    .otherwise("Não informado")
).withColumn(
    "fue_hospitalizado",
    when(col("fue_hospitalizado") == 1, "Sim")
    .when(col("fue_hospitalizado") == 2, "Não")
    .when(col("fue_hospitalizado") == 9, "Ignorado")
    .otherwise("Não Informado")
).withColumn(
    "clasificacion_final",
    when(col("clasificacion_final_id") == 5, "Descartado")
    .when(col("clasificacion_final_id") == 10, "Dengue")
    .when(col("clasificacion_final_id") == 11, "Dengue con Señales de Alarma")
    .when(col("clasificacion_final_id") == 12, "Dengue Grave")
    .when(col("clasificacion_final_id") == 13, "Chikungunya")
    .otherwise("Otro/En Investigación")
).withColumn(
    "criterio_confirmacion",
    when(col("criterio_confirmacion_id") == 1, "Laboratório")
    .when(col("criterio_confirmacion_id") == 2, "Clínico Epidemiológico")
    .when(col("criterio_confirmacion_id") == 3, "Em investigação")
    .otherwise("Ignorado") 
).withColumn(
    "resultado_serologia",
    when(col("resultado_serologia_id") == 1, "Reagente")
    .when(col("resultado_serologia_id") == 2, "Não Reagente")
    .when(col("resultado_serologia_id") == 3, "Inconclusivo")
    .when(col("resultado_serologia_id") == 4, "Não realizado")
    .otherwise("Não Informado")
).withColumn(
    "resultado_chik_1",
    when(col("resultado_chik_1_id") == 1, "Reagente")
    .when(col("resultado_chik_1_id") == 2, "Não Reagente")
    .when(col("resultado_chik_1_id") == 3, "Inconclusivo")
    .when(col("resultado_chik_1_id") == 4, "Não realizado")
    .otherwise("Não Informado")
).withColumn(
    "resultado_chik_2",
    when(col("resultado_chik_2_id") == 1, "Reagente")
    .when(col("resultado_chik_2_id") == 2, "Não Reagente")
    .when(col("resultado_chik_2_id") == 3, "Inconclusivo")
    .when(col("resultado_chik_2_id") == 4, "Não realizado")
    .otherwise("Não Informado")
)

# COMMAND ----------

df_enriquecido = df_limpio.join(
    df_geo_preparado,
    "municipio_id_6_digitos",
    "left"
)

# COMMAND ----------

df_final = df_enriquecido.select(
    # Seleccionamos las columnas deseadas en el orden final
    "fecha_notificacion",
    "ano_notificacion",
    "nome_municipio", 
    "uf",             
    "nome_estado",    
    "regiao",         
    "lat_municipio",  
    "lon_municipio",  
    "edad",
    "sexo",
    "gestante",
    "clasificacion_final",
    "criterio_confirmacion",
    "fue_hospitalizado",
    "evolucion_caso",
    "es_enfermedad_laboral",
    "sospecha_clinica_chik",
    "fecha_investigacion",
    "fecha_serologia",
    "resultado_serologia",
    "fecha_chik_s1",
    "resultado_chik_1",
    "fecha_chik_s2",
    "resultado_chik_2",
    "id_uf_infeccion",
).na.fill({
    "nome_municipio": "Desconocido",
    "uf": "Desconocido"
})

# COMMAND ----------

#escrever de novo na tabela
df_final.write.mode("overwrite").saveAsTable("union_datasets")


