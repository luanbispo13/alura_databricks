# 🏗️ Pipeline de Ingestão de Dados — `prod_ingestao_dados`

Pipeline de engenharia de dados da **Alumax** para consolidar interações de suporte multicanal. Segue a arquitetura medalhão no Databricks, alimentando tabelas analíticas para dashboards de BI e acompanhamento de CSAT.

---

## 📌 Sumário Executivo

- **Objetivo:** Ingestão contínua, limpeza e modelagem dos atendimentos dos 4 canais da Alumax.
- **Arquitetura:** Medalhão (Bronze ➔ Silver ➔ Gold) com Delta Lake e Unity Catalog.
- **Gatilho:** Event-driven via **File Arrival** (chegou arquivo no Volume, o job roda).
- **Gerenciamento:** Databricks Asset Bundles (DABs) como IaC.

## 🎓 Origem do Projeto

Este pipeline foi construído do zero como parte prática do curso **[Databricks: Pipelines de dados com PySpark e Delta Lake](https://cursos.alura.com.br/course/databricks-pipeline-dados-pyspark-delta-lake)**, oferecido pela plataforma **Alura**. O projeto consolida conceitos de ingestão, transformação e orquestração de dados modernos na nuvem.

---

## 🎯 Canais Coletados

| Canal | O que monitoramos | Métrica-chave de tempo |
| :--- | :--- | :--- |
| 💬 **WhatsApp** | Resoluções via bot automatizado | Tempo de resposta do bot |
| 📞 **Telefone** | Ligações receptivas, transferências e espera | Tempo de fila + duração da chamada |
| 📧 **E-mail** | Tickets assíncronos de suporte | Tempo de primeira resposta |
| 🌐 **Chat (Site)** | Conversas via widget do portal | Duração total da sessão |

---

## 🔍 Diagnóstico Operacional (Dados da Camada Gold)

> Fonte: `prod.gold.atendimentos_alumax` (consumido pelo notebook `aula_5_2_dash_insights.ipynb`).

### 📊 Desempenho e Volume por Canal

| Canal | Volume | Share (%) | Tempo Médio | CSAT Médio (0–10) | Status |
| :--- | :---: | :---: | :---: | :---: | :---: |
| 💬 **WhatsApp** | 2.522 | 25,2% | **1,33 min** | **8,03** | 🟢 Excelente |
| 📧 **E-mail** | 2.523 | 25,2% | 732,14 min (~12h) | 6,01 | 🟡 Regular |
| 🌐 **Chat (Site)** | 2.522 | 25,2% | *Sem telemetria* | 5,52 | 🟡 Alerta |
| 📞 **Telefone** | 2.433 | 24,3% | 7,85 min | **2,54** | 🔴 Crítico |

### 💡 Principais Descobertas

* **Operação equilibrada em volume:** Cada canal absorve cerca de 25% da demanda total.
* **Velocidade gera satisfação:** O WhatsApp lidera em CSAT (8,03) impulsionado pelo retorno ágil do bot (1,3 min).
* **Telefone é o maior gargalo:** Apesar da chamada durar menos de 8 minutos, a nota média despenca para 2,54. O problema não é tempo bruto de ligação, mas transferências excessivas (`telefone_transferencias`) e retenção na fila (`telefone_fila_espera_seg`).
* **E-mail tem tolerância:** Leva 12 horas para primeira resposta, mas o cliente tolera melhor a espera por ser um canal naturalmente assíncrono (nota 6,01).

### 🎯 Plano de Ação Recomendado

1. **Reestruturar o atendimento telefônico:** Auditar as transferências entre setores e mitigar filas para conter a insatisfação.
2. **Promover transbordo para WhatsApp:** Fortalecer o bot para resolver dúvidas frequentes no canal mais bem avaliado.
3. **Triagem automática no e-mail:** Configurar auto-respostas com base em palavras-chave para reduzir a primeira resposta de 12h para menos de 4h.
4. **Instrumentar o Chat do Site:** Implementar tracking de tempo de resposta para entender o motivo da nota baixa (5,52).

---

## 🏛️ Arquitetura de Dados (Medalhão)

O pipeline implementa a **Arquitetura Medalhão** sobre o Delta Lake, garantindo governança, rastreabilidade e qualidade progressiva dos dados em cada etapa do fluxo.

```text
📥 ORIGEM: Unity Catalog Volume (Landing de CSVs)
│
▼
🥉 BRONZE | Camada Bruta (Raw)
│  📍 Tabela: prod.bronze.atendimentos_raw
│  
│  • Motor: Databricks Auto Loader (cloudFiles)
│  • Tipo: Carga incremental via Streaming Table
│  • Foco: Histórico imutável + Schema Evolution
│
▼
🥈 SILVER | Camada Padronizada (Cleansed)
│  📍 Tabela: prod.silver.atendimentos_limpos
│  
│  • Ação: Deduplicação de IDs de atendimento (Upsert)
│  • Limpeza: Cast de tipos e tratamento de nulos
│  • Padronização: Normalização de domínios (ex: 'zap' ➔ WHATSAPP)
│
▼
🥇 GOLD | Camada de Negócio (Curated)
│  📍 Tabela: prod.gold.atendimentos_alumax
│  
│  • Negócio: Cálculo de SLA, Tempos de resposta e CSAT
│  • Otimização: Delta Lake colunar com Z-Ordering
│  • Performance: Particionamento Mensal/Anual
│
▼
📊 CONSUMO: Dashboards de BI & Databricks SQL
```

### 🕒 Colunas de Auditoria Técnica

Para garantir a linhagem perfeita dos dados, cada medalha anexa automaticamente seu próprio carimbo de data/hora de processamento:

| Camada | Coluna de Auditoria | O que ela garante? |
| :---: | :--- | :--- |
| 🥉 **Bronze** | `_ingested_at` | Rastreabilidade exata de quando o arquivo aterrissou no Lake. |
| 🥈 **Silver** | `_processed_at` | Data/hora em que o registro foi limpo e deduplicado. |
| 🥇 **Gold** | `_loaded_at` | Momento em que a métrica final foi disponibilizada para o BI. |

---

## ⚙️ Orquestração e Observabilidade

* **Gatilho (Trigger):** `File Arrival`. O pipeline é acionado por eventos — assim que um novo CSV cai no Unity Catalog Volume, o processamento inicia sem depender de agendamentos cron.
* **Monitoramento & Alertas:** Falhas na pipeline disparam alertas automáticos via e-mail direto para o time de Engenharia de Dados com o log do erro para rápida remediação.

---

## 🗂️ Estrutura do Repositório

```text
prod_ingestao_dados/
├── databricks.yml          # Definição do Bundle (DABs: jobs, pipelines e targets)
├── README.md               # Documentação técnica e operacional
├── .gitignore              # Arquivos e diretórios ignorados no versionamento
└── etl/
    ├── etl_bronze.ipynb    # Ingestão raw com Auto Loader
    ├── etl_silver.ipynb    # Regras de limpeza, deduplicação e schema
    └── etl_gold.ipynb      # Agregações de negócio e tabela analítica final
```

---

## 🚀 Como Fazer o Deploy (DABs)

### 📋 Pré-requisitos
* Databricks CLI configurado e autenticado.
* Permissão de escrita no catálogo `prod` do Unity Catalog.

### 💻 Comandos

```bash
# 1. Validar sintaxe e integridade do bundle
databricks bundle validate

# 2. Publicar recursos no ambiente de produção
databricks bundle deploy --target prod

# 3. Rodar o pipeline manualmente para teste de carga
databricks bundle run prod_ingestao_dados --target prod
```

---

## 📋 Especificação da Tabela Final

* **Tabela:** `prod.gold.atendimentos_alumax`
* **Formato:** Delta Lake
* **Chave de Partição:** `ano_mes` (derivada de `data_hora`)
* **Principais Usuários:** Engenharia de BI, Operações de Suporte e Liderança de CS.
