# Databricks notebook source
# ==================================================================================
# OBJETIVO: Carregar dados brutos de casos, limpá-los, enriquecê-los com
#           informações geográficas e salvar uma tabela base limpa.
# ==================================================================================
# Importação das bibliotecas e funções necessárias do PySpark.
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, expr
from functools import reduce

# COMMAND ----------

# --- 1. INICIALIZAÇÃO DO SPARK E CARREGAMENTO DE DADOS GEOGRÁFICOS ---
# Inicializa a sessão do Spark, que é o ponto de entrada para programar com DataFrames.
spark = SparkSession.builder.appName("workspace").getOrCreate()

# Carrega as tabelas mestre de municípios и estados.
df_municipios = spark.table("workspace.default.municipios")
df_estados = spark.table("workspace.default.estados")


# COMMAND ----------

# --- 2. PREPARAÇÃO DA TABELA MESTRE GEOGRÁFICA ---

# Une as tabelas de municípios e estados usando o código da UF.
# Um 'left join' garante que todos os municípios sejam mantidos, mesmo que não tenham um estado correspondente.
df_geo_mestre = df_municipios.join(
    df_estados,
    df_municipios.codigo_uf == df_estados.codigo_uf,
    "left"
).select(
    # Seleciona e renomeia as colunas de interesse para criar uma tabela geográfica limpa.
    df_municipios.codigo_ibge,
    df_municipios.nome.alias("nome_municipio"),
    df_municipios.latitude.alias("lat_municipio"),
    df_municipios.longitude.alias("lon_municipio"),
    df_estados.uf.alias("uf"),
    df_estados.nome.alias("nome_estado"),
    df_estados.regiao
)

# Cria uma coluna com os primeiros 6 dígitos do 'codigo_ibge'.
# Este ID de 6 dígitos é a chave que será usada para unir com os dados de casos.
df_geo_preparado = df_geo_mestre.withColumn(
    "id_municipio_6_digitos",
    expr("substring(cast(codigo_ibge as string), 1, 6)")
).dropDuplicates(["id_municipio_6_digitos"])

# COMMAND ----------

# --- 3. UNIÃO DOS DATASETS DE CASOS ANUAIS ---

# Carrega os datasets de casos de Chikungunya de diferentes anos.
df_25 = spark.table("workspace.default.chikbr_25")
df_24 = spark.table("workspace.default.chikbr_24")
df_23 = spark.table("workspace.default.chikbr_23")
df_22 = spark.table("workspace.default.chikbr_22")

# Agrupa todos os DataFrames em uma lista para facilitar a união.
dataframes_para_unir = [df_25, df_24, df_23, df_22]

# Inicializa o DataFrame unificado com o primeiro elemento da lista.
df_unificado = dataframes_para_unir[0]

# Percorre o resto da lista e une cada DataFrame.
# 'unionByName' é crucial aqui porque une as colunas por nome, não por posição,
# o que evita erros se os esquemas não forem idênticos em ordem.
for df_seguinte in dataframes_para_unir[1:]:
    df_unificado = df_unificado.unionByName(df_seguinte)


# COMMAND ----------

# --- 4. LIMPEZA E TRANSFORMAÇÃO DE DADOS ---

# Inicia o processo de limpeza e padronização do DataFrame unificado.
df_limpo = df_unificado.select(
    # Seleciona as colunas de interesse e atribui a elas um nome claro.
    col("DT_NOTIFIC").alias("data_notificacao"),
    col("NU_ANO").alias("ano_notificacao"),
    col("ID_MUNICIP").alias("id_municipio_6_digitos"),
    col("ANO_NASC").alias("ano_nascimento"),
    col("CS_SEXO").alias("sexo"),
    col("CS_GESTANT").alias("gestante"),
    col("DT_INVEST").alias("data_investigacao"),
    col("DT_CHIK_S1").alias("data_chik_s1"),
    col("DT_CHIK_S2").alias("data_chik_s2"),
    col("RES_CHIKS1").alias("resultado_chik_1_id"),
    col("RES_CHIKS2").alias("resultado_chik_2_id"),
    col("DT_SORO").alias("data_sorologia"),
    col("RESUL_SORO").alias("resultado_sorologia_id"),
    col("HOSPITALIZ").alias("foi_hospitalizado"),
    col("COUFINF").alias("id_uf_infeccao"),
    col("CLASSI_FIN").alias("classificacao_final_id"),
    col("CRITERIO").alias("criterio_confirmacao_id"),
    col("DOENCA_TRA").alias("e_doenca_trabalho"),
    col("CLINC_CHIK").alias("suspeita_clinica_chik"),
    col("EVOLUCAO").alias("evolucao_caso_id")
).withColumn(
    # Calcula a idade do paciente subtraindo o ano de nascimento do ano de notificação.
    # 'try_cast' é usado para evitar erros se os dados não forem numéricos.
    "idade",
    expr("try_cast(ano_notificacao as integer) - try_cast(ano_nascimento as integer)")
).withColumn(
    # Converte as colunas de data do formato 'yyyyMMdd' para um tipo de dado de data padrão.
    # 'try_to_date' lida de forma segura com valores que não correspondem ao formato.
    "data_notificacao", expr("try_to_date(data_notificacao, 'yyyyMMdd')")
).withColumn(
    "data_investigacao", expr("try_to_date(data_investigacao, 'yyyyMMdd')")
).withColumn(
    "data_chik_s1", expr("try_to_date(data_chik_s1, 'yyyyMMdd')")
).withColumn(
    "data_chik_s2", expr("try_to_date(data_chik_s2, 'yyyyMMdd')")
).withColumn(
    "data_sorologia", expr("try_to_date(data_sorologia, 'yyyyMMdd')")
).withColumn(
    # Decodifica a coluna 'sexo' de 'M'/'F' para valores mais descritivos.
    "sexo", when(col("sexo") == 'M', "Masculino").when(col("sexo") == 'F', "Feminino").otherwise("Ignorado")
).withColumn(
    # Decodifica o ID numérico da evolução do caso para uma descrição textual.
    "evolucao_caso",
    when(col("evolucao_caso_id") == 1, "Cura")
    .when(col("evolucao_caso_id") == 2, "Óbito pela doença")
    .when(col("evolucao_caso_id") == 3, "Óbito por outras causas")
    .when(col("evolucao_caso_id") == 4, "Óbito em investigação")
    .when(col("evolucao_caso_id") == 9, "Ignorado")
    .otherwise("Não informado")
).withColumn(
    # Decodifica o ID de hospitalização para 'Sim'/'Não'.
    "foi_hospitalizado",
    when(col("foi_hospitalizado") == 1, "Sim")
    .when(col("foi_hospitalizado") == 2, "Não")
    .when(col("foi_hospitalizado") == 9, "Ignorado")
    .otherwise("Não Informado")
).withColumn(
    # Mapeia o ID de classificação final para seu significado (Descartado, Dengue, Chikungunya, etc.).
    "classificacao_final",
    when(col("classificacao_final_id") == 5, "Descartado")
    .when(col("classificacao_final_id") == 10, "Dengue")
    .when(col("classificacao_final_id") == 11, "Dengue com Sinais de Alarme")
    .when(col("classificacao_final_id") == 12, "Dengue Grave")
    .when(col("classificacao_final_id") == 13, "Chikungunya")
    .otherwise("Outro/Em Investigação")
).withColumn(
    # Decodifica o critério de confirmação (Laboratorial, Clínico-epidemiológico, etc.).
    "criterio_confirmacao",
    when(col("criterio_confirmacao_id") == 1, "Laboratorial")
    .when(col("criterio_confirmacao_id") == 2, "Clínico-epidemiológico")
    .when(col("criterio_confirmacao_id") == 3, "Em investigação")
    .otherwise("Ignorado") 
).withColumn(
    # Decodifica os resultados dos testes de sorologia.
    "resultado_sorologia",
    when(col("resultado_sorologia_id") == 1, "Reagente")
    .when(col("resultado_sorologia_id") == 2, "Não Reagente")
    .when(col("resultado_sorologia_id") == 3, "Inconclusivo")
    .when(col("resultado_sorologia_id") == 4, "Não realizado")
    .otherwise("Não Informado")
).withColumn(
    # Decodifica os resultados do primeiro teste de Chikungunya.
    "resultado_chik_1",
    when(col("resultado_chik_1_id") == 1, "Reagente")
    .when(col("resultado_chik_1_id") == 2, "Não Reagente")
    .when(col("resultado_chik_1_id") == 3, "Inconclusivo")
    .when(col("resultado_chik_1_id") == 4, "Não realizado")
    .otherwise("Não Informado")
).withColumn(
    # Decodifica os resultados do segundo teste de Chikungunya.
    "resultado_chik_2",
    when(col("resultado_chik_2_id") == 1, "Reagente")
    .when(col("resultado_chik_2_id") == 2, "Não Reagente")
    .when(col("resultado_chik_2_id") == 3, "Inconclusivo")
    .when(col("resultado_chik_2_id") == 4, "Não realizado")
    .otherwise("Não Informado")
)

# COMMAND ----------

# --- 5. ENRIQUECIMENTO COM DADOS GEOGRÁFICOS ---

# Une os dados limpos de casos com a tabela geográfica preparada.
# O 'left join' garante que todos os registros de casos sejam mantidos,
# mesmo que seu 'id_municipio_6_digitos' não seja encontrado na tabela geográfica.
df_enriquecido = df_limpo.join(
    df_geo_preparado,
    "id_municipio_6_digitos",
    "left"
)

# COMMAND ----------

# --- 6. PREPARAÇÃO FINAL E TRATAMENTO DE NULOS ---

# Seleciona as colunas finais na ordem desejada para a tabela de destino.
df_final = df_enriquecido.select(
    "data_notificacao",
    "ano_notificacao",
    "nome_municipio", 
    "uf",             
    "nome_estado",    
    "regiao",         
    "lat_municipio",  
    "lon_municipio",  
    "idade",
    "sexo",
    "gestante",
    "classificacao_final",
    "criterio_confirmacao",
    "foi_hospitalizado",
    "evolucao_caso",
    "e_doenca_trabalho",
    "suspeita_clinica_chik",
    "data_investigacao",
    "data_sorologia",
    "resultado_sorologia",
    "data_chik_s1",
    "resultado_chik_1",
    "data_chik_s2",
    "resultado_chik_2",
    "id_uf_infeccao"
).na.fill({
    # Preenche os valores nulos que podem ter surgido do 'left join'.
    # Se um município não for encontrado, seu nome e UF serão marcados como "Desconhecido".
    "nome_municipio": "Desconhecido",
    "uf": "Desconhecido"
})


# COMMAND ----------

# --- 7. ESCRITA DA TABELA FINAL ---

# Salva o DataFrame final como uma tabela no workspace.
# O modo 'overwrite' substituirá a tabela se ela já existir, o que permite
# executar este script várias vezes sem erros.
df_final.write.mode("overwrite").saveAsTable("casos_unificados_br")
