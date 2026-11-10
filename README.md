%md
# 🏗️ Pipeline de Ingestão de Dados — `prod_ingestao_dados`

> Pipeline corporativo de processamento de interações de atendimento ao cliente da **Alumax**, construído sobre a **Arquitetura Medalhão** no **Databricks Lakehouse**.

---

## 📋 Visão Geral do Projeto

O `prod_ingestao_dados` é um pipeline de dados end-to-end que processa **interações de atendimento ao cliente** da empresa **Alumax** através de quatro canais:

| Canal | Descrição |
|------|-----------|
| 📞 **Telefone** | Chamadas telefónicas com registo de duração, fila de espera e transferências |
| 💬 **WhatsApp** | Mensagens com tempo de resposta do bot automatizado |
| 🌐 **Chat (Site)** | Atendimentos via chat no website da Alumax |
| 📧 **E-mail** | Comunicações por e-mail com métrica de tempo de primeira resposta |

O pipeline utiliza a **Arquitetura Medalhão** (Bronze → Silver → Gold) no Databricks, garantindo rastreabilidade, qualidade e governança em cada camada. Os dados processados alimentam diretamente os **dashboards de BI** e análises de experiência do cliente.

### 📊 Resumo Executivo de Insights

> Extraído do notebook de análise `aula_5_2_dash_insights.ipynb`, que consome a tabela final `prod.gold.atendimentos_alumax`.

#### Satisfação por Canal

| Canal | Satisfação Média (0–10) | Volume | Percentagem |
|-------|-------------------------|--------|-------------|
| 💬 WHATSAPP | **8.03** | 2.522 | 25.2% |
| 📧 EMAIL | 6.01 | 2.523 | 25.2% |
| 🌐 CHAT_SITE | 5.52 | 2.522 | 25.2% |
| 📞 TELEFONE | **2.54** | 2.433 | 24.3% |

* **WhatsApp** é o canal com **maior satisfação** (8.03/10), muito à frente dos restantes.
* **Telefone** tem a **pior satisfação** (2.54/10) — sinal crítico de alarme, pois representa ~24% do volume total.
* A evolução mensal mantém-se relativamente estável, sem tendências de melhoria acentuadas.

#### Volume de Chamados por Canal

* A distribuição entre canais é **quase uniforme** (~25% cada), indicando uma operação multicanal equilibrada.
* Não se identificam tendências de crescimento ou queda acentuada num canal específico ao longo dos meses.

#### Eficiência de Tempo por Canal

| Canal | Tempo Médio | Satisfação |
|-------|-------------|------------|
| 💬 WHATSAPP | **1.33 min** | 8.03 |
| 📞 TELEFONE | 7.85 min | 2.54 |
| 📧 EMAIL | 732.14 min (~12h) | 6.01 |

* **WhatsApp** é claramente o canal mais **rápido** (1.33 min de resposta do bot), o que explica a sua alta satisfação.
* **E-mail** é o mais **lento** (~12 horas para primeira resposta), mas mantém satisfação moderada (6.01).
* **Telefone** combina tempo moderado (7.85 min) com a **pior satisfação** — o problema não é apenas o tempo, mas possivelmente a qualidade do atendimento, número de transferências ou dificuldade de resolução.
* **Chat (Site)** não possui métrica de tempo direta na tabela, mas apresenta satisfação baixa (5.52), sugerindo necessidade de investigação adicional.

#### Correlação: Tempo de Espera vs Satisfação

Existe uma **correlação inversa clara** entre tempo de atendimento e satisfação:

* WhatsApp (1.33 min → 8.03 sat) — menor tempo, maior satisfação ✅
* E-mail (732 min → 6.01 sat) — maior tempo, satisfação moderada ⚠️
* Telefone (7.85 min → 2.54 sat) — **exceção à regra**: mesmo com tempo razoável, a satisfação é muito baixa ❌

A exceção do Telefone sugere que **a satisfação não depende apenas do tempo**, mas também de fatores qualitativos como:
* Número de transferências (`telefone_transferencias`)
* Tempo de fila de espera (`telefone_fila_espera_seg`)
* Qualidade da resolução

#### Recomendações de Negócio

1. **Investir em WhatsApp como canal principal**: Mais rápido, mais satisfatório e representa 25% do volume. Expandir capacidade e promover migração de clientes para este canal.
2. **Reforma urgente do canal Telefone**: Satisfação de 2.54/10 é criticamente baixa. Investigar causas raiz (transferências excessivas, tempo de fila, qualidade do atendimento) e implementar plano de melhoria.
3. **Otimizar tempo de resposta do E-mail**: 12 horas de espera é excessivo. Considerar respostas automáticas/triagem inteligente para reduzir o tempo de primeira resposta.
4. **Investigar Chat (Site)**: Satisfação baixa (5.52) sem métrica de tempo disponível. Recolher dados de tempo de resposta e comparar com os outros canais.
5. **Implementar SLAs por canal**: Definir metas de tempo de resposta alinhadas com as expectativas dos clientes (ex: WhatsApp < 2 min, Telefone < 5 min, E-mail < 4 horas).

---

## 🏅 Arquitetura de Dados (Medalhão)

O pipeline segue o padrão **Medallion Architecture** com três camadas de processamento progressivo:

```
📁 Unity Catalog Volume (CSV)
    │
    ▼
┌─────────────────────────────────┐
│  🥉 BRONZE — Ingestão Raw       │
│  Auto Loader → Streaming Table  │
└─────────────────────────────────┘
    │
    ▼
�─────────────────────────────────┐
│  🥈 SILVER — Limpeza & Padrão   │
│  Deduplicação, tipos, validação │
└─────────────────────────────────┘
    │
    ▼
┌─────────────────────────────────┐
│  🥇 GOLD — Regras de Negócio    │
│  prod.gold.atendimentos_alumax  │
└─────────────────────────────────┘
    │
    ▼
📊 Dashboards & Análises de BI
```

### 🥉 Camada Bronze — Ingestão

* **Origem:** Arquivos CSV aterrissados em **Unity Catalog Volumes**.
* **Mecanismo:** **Databricks Auto Loader** (`cloudFiles`) para deteção automática de novos ficheiros.
* **Tipo:** Streaming Table com ingestion incremental.
* **Características:**
  * Preserva os dados no formato original (raw) sem transformações.
  * Suporta schema evolution automático.
  * Idempotente — reprocessamento seguro sem duplicação.

### 🥈 Camada Silver — Limpeza de Dados

* **Objetivo:** Padronização, limpeza e enriquecimento dos dados da camada Bronze.
* **Transformações principais:**
  * **Deduplicação** de registos.
  * **Padronização de tipos** (casting de colunas).
  * **Validação de domínio** (valores nulos, ranges, formatos).
  * **Normalização de canal** (valores sujos → `WHATSAPP`, `TELEFONE`, `EMAIL`, `CHAT_SITE`).
  * **Tratamento de nulos** e valores inválidos.

### 🥇 Camada Gold — Regras de Negócio

* **Objetivo:** Aplicação de regras de negócio e criação da tabela analítica final.
* **Tabela final:** `prod.gold.atendimentos_alumax`
* **Características:**
  * **Particionamento** por período (mês/ano) para eficiência de consulta.
  * **Métricas calculadas** e colunas derivadas prontas para consumo de BI.
  * **Alimenta diretamente** os dashboards e relatórios de análise.
  * Esquema otimizado para leitura analítica (colunar).

### 🕐 Colunas de Auditoria

Todas as camadas (Bronze, Silver e Gold) incluem **colunas de auditoria** com os respetivos **timestamps de processamento**, garantindo rastreabilidade completa do ciclo de vida dos dados:

| Coluna | Descrição |
|--------|-----------|
| `_ingested_at` | Timestamp de ingestão na camada Bronze |
| `_processed_at` | Timestamp de processamento na camada Silver |
| `_loaded_at` | Timestamp de carregamento na camada Gold |

---

## ⚡ Gatilhos e Alertas

### Gatilho: File Arrival

O job **não utiliza cronograma fixo** (cron schedule). Em vez disso, é acionado pelo gatilho **File Arrival**:

* 📂 O Databricks monitora continuamente a chegada de novos arquivos CSV no **Unity Catalog Volume**.
* ⚡ Assim que um novo arquivo é detetado, o job é **automaticamente disparado**.
* 🔄 Isto garante **baixa latência** entre a chegada dos dados e a sua disponibilidade nos dashboards.

**Vantagens do gatilho File Arrival:**

* ✅ Processamento em **tempo quase real** (event-driven).
* ✅ **Sem janelas de espera** desnecessárias (vs. agendamento fixo).
* ✅ **Escala automaticamente** com o volume de arquivos recebidos.

### Alertas Automáticos

* 📧 **Falhas no job** disparam um **alerta automático por e-mail** para a equipa de engenharia de dados.
* 🚨 O alerta inclui detalhes do erro, facilitando o diagnóstico rápido.
* 📋 Permite **resposta proativa** a incidentes antes que impactem os dashboards.

---

## 📦 Infraestrutura como Código (IaC)

O pipeline é gerenciado integralmente via **Databricks Asset Bundles (DABs)** através do ficheiro de configuração `databricks.yml`.

### Estrutura do Repositório

```
prod_ingestao_dados/
├── databricks.yml          # Configuração principal do DAB
├── resources/
│   ├── bronze/             # Notebooks da camada Bronze
│   │   └── ingestion.py
│   ├── silver/             # Notebooks da camada Silver
│   │   └── cleansing.py
│   └── gold/               # Notebooks da camada Gold
│       └── business_rules.py
├── tests/                  # Testes unitários e de integração
├── README.md               # Este ficheiro
└── .gitignore
```

### Databricks Asset Bundles (DABs)

* **Ficheiro principal:** `databricks.yml`
* **Objetivo:** Definir e versionar toda a infraestrutura do pipeline como código.
* **Vantagens:**
  * 🔁 **Reprodutibilidade:** Ambientes idênticos em dev, staging e produção.
  * 📝 **Versionamento:** Toda a configuração é versionada no Git.
  * 🚀 **Deploy automatizado:** `databricks bundle deploy` para publicar alterações.
  * 🧪 **Validação:** `databricks bundle validate` antes do deploy.
  * 🔧 **Gestão centralizada** de jobs, pipelines e recursos.

---

## 🔗 Tabela de Dados Final

| Propriedade | Valor |
|-------------|-------|
| **Catálogo** | `prod` |
| **Schema** | `gold` |
| **Tabela** | `atendimentos_alumax` |
| **Nome completo** | `prod.gold.atendimentos_alumax` |
| **Particionamento** | Por mês/ano (`data_hora`) |
| **Consumidores** | Dashboards de BI, Notebooks de análise |

---

## 🛠️ Tecnologias Utilizadas

| Tecnologia | Uso |
|------------|-----|
| **Databricks Lakehouse** | Plataforma de processamento de dados |
| **Unity Catalog** | Governança e segurança de dados |
| **Unity Catalog Volumes** | Armazenamento de ficheiros CSV |
| **Databricks Auto Loader** | Ingestão incremental de ficheiros |
| **Delta Lake** | Formato de armazenamento transacional |
| **Databricks Asset Bundles** | Infraestrutura como Código (IaC) |
| **Lakeflow Jobs** | Orquestração e agendamento |
| **Python / PySpark** | Transformações de dados |

---

## 🚀 Como Executar

### Pré-requisitos

* Databricks CLI instalado (`databricks`)
* Acesso ao workspace do Databricks
* Permissões no Unity Catalog (`prod`)

### Deploy do Pipeline

```bash
# Validar a configuração do bundle
databricks bundle validate

# Deploy para o ambiente de produção
databricks bundle deploy --target prod

# Executar o job manualmente (se necessário)
databricks bundle run prod_ingestao_dados --target prod
```

---

## 👥 Equipa

* **Engenharia de Dados** — Manutenção e evolução do pipeline
* **Análise de BI** — Consumo dos dados e geração de insights
* **Operações de Atendimento** — Stakeholders das métricas de satisfação

---

## 📄 Licença

Propriedade da **Alumax**. Uso interno restrito.

---

> _Pipeline construído seguindo as melhores práticas de engenharia de dados corporativa no Databricks._
