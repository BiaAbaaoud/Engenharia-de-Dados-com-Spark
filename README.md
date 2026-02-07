# ⚡ Pipeline de Big Data: WeFitness Analytics

Este projeto simula um ecossistema de dados para uma rede fictícia de academias (WeFitness). O objetivo é processar grandes volumes de dados de check-ins para extrair inteligência financeira, categorizar clientes e preparar os dados para dashboards executivos.

## 🚀 Objetivo
Transformar dados brutos (Raw Data) em insights acionáveis, utilizando processamento distribuído com **Apache Spark** e seguindo as melhores práticas de Engenharia de Dados com a **Arquitetura Medalhão**.

---

## 🏗️ Estrutura do Pipeline

O pipeline é dividido em 3 etapas principais:

1.  **Ingestão (Bronze):** Geração de 100.000 registros sintéticos e armazenamento em formato **Parquet**.
2.  **Transformação (Silver):** Limpeza, filtragem de valores e criação de categorias (Standard e Premium) através de Feature Engineering.
3.  **Agregação (Gold):** Consolidação de métricas (Ticket Médio e Faturamento Total) e carga final no banco de dados **SQLite**.



---

## 📂 Descrição dos Arquivos

| Arquivo | Função |
| :--- | :--- |
| `gerar_big_data_wefitness.py` | Gera os dados brutos e cria a Camada Bronze. |
| `transformar_silver.py` | Refina os dados e aplica regras de negócio (Camada Silver). |
| `gerar_gold_final.py` | Agrega os dados e faz o "Load" no Banco SQL (Camada Gold). |
| `ler_dados_spark.py` | Script utilitário para validar a leitura dos arquivos Parquet. |
| `wefitness_analytics.db` | Banco de Dados SQLite final com os resultados prontos para BI. |

---

## 🛠️ Ferramentas Utilizadas

* **Python 3.13**
* **Apache Spark (PySpark)**
* **Hadoop (Winutils)** para execução em ambiente Windows.
* **Pandas** para ponte de dados SQL.
* **SQLite** como Data Warehouse simplificado.

---

## 🏃 Como rodar o projeto

1.  Certifique-se de ter o **Spark** instalado e configurado.
2.  Clone o repositório: `git clone https://github.com/seu-usuario/Engenharia-de-Dados-com-Spark.git`
3.  Execute os scripts na ordem:
    ```bash
    python gerar_big_data_wefitness.py
    python transformar_silver.py
    python gerar_gold_final.py
    ```

---

## ❓ FAQ - Perguntas Frequentes

**1. Por que usar Parquet e não CSV?** O Parquet é um formato colunar que reduz o espaço em disco e acelera a leitura do Spark em até 10x comparado ao CSV.

**2. O que é a Arquitetura Medalhão?** É uma estrutura de organização de dados (Bronze/Silver/Gold) que garante qualidade e linhagem dos dados durante o processo.

**3. O Spark roda bem no Windows?** Sim, desde que configurado com os binários do Hadoop (`winutils.exe` e `hadoop.dll`) nas variáveis de ambiente.

**4. O projeto escala para milhões de linhas?** Sim! O código foi escrito usando a API de DataFrames do Spark, que distribui o processamento independente do volume.

**5. Por que usar SQLite na camada Gold?** Para simular a entrega final em um ambiente SQL relacional, facilitando a conexão com ferramentas de BI como Power BI ou Tableau.

**6. Como as categorias VIP foram definidas?** Check-ins acima de R$ 80,00 foram classificados como Premium, e os demais acima de R$ 50,00 como Standard.

---

## 👩‍💻 Desenvolvedora
**Bia Abaaoud** 