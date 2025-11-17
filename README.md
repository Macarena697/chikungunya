# Sumário

1. **DIAGNÓSTICO E TEORIZAÇÃO**  
   1.1. Identificação das partes interessadas e parceiros  
   1.2. Problemática e/ou problemas identificados  
   1.3. Justificativa  
   1.4. Objetivos/resultados/efeitos a serem alcançados  
   1.5. Referencial teórico  

2. **PLANEJAMENTO E DESENVOLVIMENTO DO PROJETO**  
   2.1. Plano de trabalho  
   2.2. Envolvimento do público participante  
   2.3. Grupo de trabalho  
   2.4. Metas, critérios ou indicadores de avaliação  
   2.5. Recursos previstos  
   2.6. Detalhamento técnico  

3. **ENCERRAMENTO DO PROJETO**  
   3.1. Relatório Coletivo  
   3.2. Avaliação de reação da parte interessada  
   3.3. Relato de Experiência Individual  
   3.4. Contextualização  
   3.5. Metodologia  
   3.6. Resultados e Discussão  
   3.7. Reflexão Aprofundada  
   3.8. Considerações Finais  

---

## DIAGNÓSTICO E TEORIZAÇÃO

### Identificação das partes interessadas e parceiros

**Partes interessadas:**  
A população brasileira, especialmente nas áreas mais afetadas pelo aumento de casos de chikungunya. O projeto aborda dados de várias regiões do Brasil, considerando diferentes faixas etárias, perfis socioeconômicos e localizações geográficas. Profissionais de saúde pública, incluindo médicos, enfermeiros, gestores de saúde e pesquisadores, também são partes-chave.

**Parceiros:**  
- Secretaria de Saúde de São José  
- Outras Secretarias de Saúde estaduais e municipais  
- Universidades  
- Centros de pesquisa sobre doenças transmitidas por mosquitos  
- Ministério da Saúde  

### Problemática e/ou problemas identificados

O Brasil tem enfrentado um aumento alarmante de casos de chikungunya, chegando a 587% em algumas regiões. O principal problema é a ausência de um modelo preditivo eficiente para identificar áreas de risco e auxiliar na alocação de recursos e prevenção.

### Justificativa

O uso de Big Data para analisar e prever surtos de chikungunya é pertinente, pois permite aplicar conhecimentos adquiridos no curso de Tópicos de Big Data em Python de maneira prática, com impacto direto na saúde pública. O Databricks será utilizado para analisar os datasets e identificar padrões de região, sintomas e condição social.

### Objetivos/resultados/efeitos a serem alcançados

- **Objetivo 1:** Analisar dados dos anos anteriores e atuais sobre os casos de chikungunya no Brasil para identificar padrões de disseminação e fatores de risco.
- **Objetivo 2:** Desenvolver e validar um modelo preditivo capaz de prever surtos de chikungunya em diferentes regiões do Brasil.
- **Objetivo 3:** Disseminar os resultados do projeto para as partes interessadas, auxiliando na implementação de políticas públicas de prevenção e controle da doença.

### Referencial teórico

O projeto apoia-se em três bases de dados principais:  
- **DATASUS:** Dados oficiais de notificação, vigilância e monitoramento de doenças no Brasil.  
- **SINAN:** Manual/dicionário detalhando campos, variáveis e critérios de preenchimento para dengue/chikungunya.  
- **Municípios brasileiros:** Base complementar para referenciar e validar os códigos de identificação municipais presentes nos datasets.

A junção dessas fontes, aliada a dados climáticos externos obtidos via API (Meteostat), configura um referencial teórico-metodológico robusto. A teoria de Big Data aplicada à saúde pública exige:  
1. Coleta de dados de fontes confiáveis  
2. Compreensão da estrutura e qualidade dos dados  
3. Aplicação de técnicas analíticas (séries temporais e machine learning) para extração de padrões e predição

**Referências:**  
- Ministério da Saúde. “Dicionário de Dados SINAN Online: Dengue/Chikungunya.”  
- DATASUS Transferência de Arquivos.  
- Meteostat: “Historical Weather and Climate Data API”.  
- KITCHIN, R. The Data Revolution.  
- MURDOCH, T. B.; DETSKY, A. S. “The Inevitable Application of Big Data to Health Care.”  
- HAN, J.; KAMBER, M.; PEI, J. Data Mining: Concepts and Techniques.

---

## PLANEJAMENTO E DESENVOLVIMENTO DO PROJETO

### Plano de trabalho

**Cronograma:**  
- **Fase 1 (Mês 1):** Coleta de dados de surtos anteriores e em tempo real  
- **Fase 2 (Mês 2-3):** Análise exploratória e pré-processamento dos dados  
- **Fase 3 (Mês 4-5):** Desenvolvimento do modelo preditivo  
- **Fase 4 (Mês 6):** Validação do modelo e elaboração de relatórios  
- **Fase 5 (Mês 7):** Disseminação dos resultados

**Responsáveis:** Cada fase terá um responsável específico do grupo.

### Envolvimento do público participante

Os participantes foram envolvidos indiretamente, por meio de feedback acadêmico e orientações de profissionais da saúde consultados informalmente sobre o dashboard. Visualizações preliminares foram compartilhadas com colegas da área da saúde para opiniões sobre clareza, utilidade e aplicabilidade dos indicadores. Reuniões semanais com o professor avaliaram o andamento.

**Estratégias de mobilização:**  
- Discussão aberta em sala de aula  
- Coleta de sugestões por formulários  
- Análise do que profissionais da saúde consideram mais útil em visualizações epidemiológicas

### Grupo de trabalho

- **Gustavo Da Silva Alves Madruga:** Dashboard final, análise dos dados, busca dos datasets e roteiro de extensão  
- **Macarena Rodriguez:** Programação no Databricks com Spark, integração dos datasets, limpeza e preparação das bases  
- **Nikolas Rodrigues da Cruz:** Forecast, apoio na programação e busca de datasets

### Metas, critérios ou indicadores de avaliação

- Limpeza e preparação de dados com preservação de ao menos 85% das linhas válidas  
- Enriquecer a base principal com pelo menos duas variáveis externas (temperatura e precipitação)  
- Desenvolver modelo preditivo com acurácia superior a 90%  
- Dashboard compreensível e útil para a maioria dos avaliadores  
- Apresentar análises que evidenciem padrões temporais e regionais relevantes

### Recursos previstos

- **Materiais:** Computadores, Databricks, armazenamento em nuvem, acesso às bases públicas  
- **Recursos humanos:** Equipe de três discentes e orientação do professor  
- **Tecnológicos:** Python, PySpark, Spark, Pandas, Statsmodels, ferramentas de visualização

### Detalhamento técnico do projeto

O projeto seguiu um pipeline de Big Data robusto no Databricks:

1. **Ingestão e União de Dados:** Carregamento de múltiplos datasets anuais de Chikungunya (2022-2025) e unificação usando `unionByName`.
2. **Limpeza e Transformação (ETL):** PySpark para tratamento detalhado dos dados, conversão de tipos, cálculo de idade, padronização de datas e decodificação de variáveis categóricas.
3. **Enriquecimento de Dados:** Integração de dados geográficos e climáticos (temperatura média e precipitação) via API da Meteostat.
4. **Modelagem Preditiva:** Modelo de série temporal (Regressão Linear - OLS) para prever contagem de casos mensais, usando tendência e sazonalidade anual.
5. **Dashboard Interativo:** Visualização dos resultados, KPIs gerais, distribuição de casos por região/faixa etária e correlação entre casos, temperatura e precipitação.

---

## ENCERRAMENTO DO PROJETO

### Relatório Coletivo

O projeto permitiu compreender como Big Data pode apoiar a saúde pública brasileira. A integração de diferentes bases nacionais forneceu uma visão abrangente da chikungunya, permitindo observar variações, tendências e padrões epidemiológicos. O Databricks com PySpark foi essencial para manipular grandes volumes de dados. O dashboard final se mostrou adequado para auxiliar profissionais da saúde na visualização dos dados e tomada de decisões. O grupo concluiu que análises avançadas podem contribuir diretamente para vigilância epidemiológica e políticas públicas.

### Avaliação de reação da parte interessada

A avaliação foi realizada por meio de feedback de profissionais da saúde e estudantes da área. Os participantes destacaram:

- Clareza dos gráficos  
- Utilidade do dashboard  
- Relevância da análise temporal e regional  
- Potencial do modelo de previsão para uso prático

---

## Relato de Experiência Individual

### Gustavo Da Silva Alves Madruga

- **Contextualização:** Atuei na análise dos dados, construção do dashboard e desenvolvimento do roteiro.
- **Metodologia:** Trabalhei com Python, Databricks, PySpark, ferramentas de visualização e datasus.
- **Resultados:** Aprimorei minha capacidade analítica e técnica, transformando dados complexos em visualizações claras.
- **Reflexão:** Percebi o impacto real da ciência de dados na saúde pública.
- **Considerações finais:** Futuramente, integrar variáveis climáticas ampliaria a precisão do modelo.

---

### Macarena Rodriguez

- **Contextualização:** Líder técnica do projeto, responsável pela arquitetura e execução do pipeline de dados, organização do fluxo de trabalho e garantia da qualidade dos dados.
- **Metodologia:** Utilizei PySpark no Databricks para construir o pipeline de ETL, unificação de datasets e decodificação de variáveis. Enriqueci os dados com integração à API da Meteostat.
- **Resultados:** Criação da tabela-base robusta, limpa e enriquecida, servindo como fonte para o dashboard e modelo preditivo. Descoberta de correlações entre casos e fatores ambientais.
- **Reflexão:** Experiência de aprendizado técnico e de liderança. A complexidade do ETL e integração da API reforçou a importância do planejamento e atenção aos detalhes. Liderança ensinou sobre comunicação, delegação e motivação da equipe.
- **Considerações finais:** Para projetos futuros, implementar pipelines com maior automação e expandir integração de APIs para incluir variáveis socioeconômicas ou de mobilidade urbana.

---

### Nikolas Rodrigues da Cruz

- **Contextualização:** Atuei na criação e avaliação do modelo Random Forest.
- **Metodologia:** Usei Python, Spark e datasus.
- **Resultados:** Boa acurácia e compreensão aprofundada de ML.
- **Reflexão:** Aplicar teoria em um problema real de saúde pública foi enriquecedor.
- **Considerações finais:** Futuras versões podem testar modelos mais avançados.