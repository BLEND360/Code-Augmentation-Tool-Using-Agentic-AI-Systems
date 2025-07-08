# Code-Augmentation-Tool-Using-Agentic-AI-Systems

## Introduction
The **Code Augmentation Tool** is an AI-driven optimization and documentation engine that transforms inefficient SQL queries, specifically in the **Snowflake SQL dialect**, into optimized **ANSI SQL** queries ready for execution in **Databricks**. Using a multi-agent system architecture, the tool improves query performance, readability, accuracy, and maintainability.

## Objectives
- Translate Snowflake SQL queries into ANSI SQL.
- Optimize queries using a coordinated agent system.
- Automatically generate documentation for the optimized queries.
- Validate the equivalence and performance improvements of the optimized queries.

## Architecture Overview

### Core Workflow:
1. **User Input**: Submit a Snowflake SQL query.
2. **Query Translation Agents**:
   - **AST Conversion Agent**: Parses the query into a structured Abstract Syntax Tree.
   - **ANSI SQL Translater Agent**: Converts Snowflake-specific syntax into ANSI SQL.
   - **Syntax Validation Agent**: Ensures correctness and compatibility.
3. **Query Optimization Agents**:
   - **Join & Aggregation Optimization Agent**
   - **Query Simplification Agent**
   - **Data Filtering Optimization Agent**
   - **Coordinator Agent**: Integrates and resolves optimizations.
4. **Documentation Agent**: Produces human-readable explanations of the final optimized query.
5. **Validation Engine**: Compares the original and optimized queries to ensure correctness and quantify performance gains.

## Dataset
The system is evaluated using a real-world eCommerce dataset from a large Brazilian marketplace. It includes over 100,000 records and is structured for advanced SQL analysis. The dataset contains the following relational tables:

- `customers`
- `orders`
- `order_items`
- `products`
- `sellers`
- `order_payments`
- `order_reviews`
- `geolocation`

## Features
- **AST Parsing**: Builds an abstract representation of the input query.
- **Snowflake-to-ANSI Translation**: Accurately translates Snowflake-specific clauses.
- **Multi-Agent Optimization**: Specialized agents for join handling, filtering, and simplification.
- **Documentation Generation**: Explains intent, structure, and performance decisions in natural language.
- **Validation Engine**:
  - Ensures logical equivalence
  - Benchmarks query performance
  - Handles precision and format normalization

## Installation
The project uses `pyproject.toml` and [uv](https://github.com/astral-sh/uv) for dependency management:

```bash
uv sync
```

### Development Setup

For development dependencies:

```bash
uv sync --group dev
```

## Dependencies

Main packages include:

 - `pandas`
 - `numpy`
 - `langchain-core`
 - `langchain-openai`
 - `langgraph`
 - `databricks`
 - `vstreamlit`
 - `pyyaml`
 - `requests`
 - `snowflake-connector-pythonv`


## Run the app locally

Make sure your virtual environment is activated and dependencies are installed via uv, then run:

```bash
streamlit run app.py
```

This will open a web interface where you can paste a Snowflake SQL query and receive a fully optimized ANSI SQL version along with detailed documentation.