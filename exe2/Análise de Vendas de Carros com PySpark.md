
# **📌 Exercício: Análise de Vendas de Carros com PySpark** 🚗  

Uma concessionária pretende analisar os dados das suas vendas de automóveis utilizando PySpark. O objetivo é estruturar os dados e responder a algumas questões sobre os veículos vendidos.  

---

## **1️⃣ Criar a base de dados e a tabela de vendas**  
Escreve um código PySpark para:  
✅ Criar uma **base de dados** chamada `concessionaria`.  
✅ Criar uma **tabela** chamada `vendas_carros` com as seguintes colunas:  

| Nome da Coluna      | Tipo de Dados | Descrição |
|---------------------|--------------|------------|
| `id_venda`         | INT          | Identificador único da venda |
| `cliente`          | STRING       | Nome do cliente |
| `marca`            | STRING       | Marca do carro vendido |
| `modelo`           | STRING       | Modelo do carro |
| `ano`              | INT          | Ano de fabrico |
| `preco`            | FLOAT        | Preço do carro (€) |
| `quantidade`       | INT          | Número de unidades vendidas |
| `metodo_pagamento` | STRING       | Método de pagamento utilizado |

Insere os seguintes dados na tabela:  

| id_venda | cliente         | marca    | modelo       | ano  | preco   | quantidade | metodo_pagamento  |
|----------|----------------|----------|-------------|------|---------|------------|-------------------|
| 1        | João Almeida   | BMW      | Série 3     | 2022 | 45000.0 | 1          | Transferência Bancária |
| 2        | Marta Costa    | Mercedes | Classe A    | 2023 | 38000.0 | 1          | Cartão de Crédito  |
| 3        | Tiago Silva    | Audi     | A4          | 2021 | 42000.0 | 1          | Financiamento       |
| 4        | Ana Pereira    | Renault  | Clio        | 2020 | 18000.0 | 2          | Cartão de Crédito  |
| 5        | Luís Fernandes | Tesla    | Model 3     | 2023 | 52000.0 | 1          | Transferência Bancária |

---

## **2️⃣ Consultas sobre os dados**  
Agora, com base na tabela `vendas_carros`, resolve os seguintes desafios utilizando **SQL no PySpark**.

---

### **a) Listar todas as vendas pagas com "Cartão de Crédito"**  
**Código PySpark:**  
```python
spark.sql("""
    SELECT * FROM vendas_carros
    WHERE metodo_pagamento = 'Cartão de Crédito'
""").show()
```
**Explicação:**  
Utilizamos a cláusula `WHERE` para filtrar os registos da tabela, mostrando apenas as vendas em que o método de pagamento foi **Cartão de Crédito**.

---

### **b) Ordenar os carros vendidos pelo preço, do mais caro para o mais barato**  
**Código PySpark:**  
```python
spark.sql("""
    SELECT * FROM vendas_carros
    ORDER BY preco DESC
""").show()
```
**Explicação:**  
A cláusula `ORDER BY preco DESC` organiza os carros vendidos por preço, do mais caro para o mais barato.

---

### **c) Calcular o valor total gasto por cada cliente**  
**Código PySpark:**  
```python
spark.sql("""
    SELECT cliente, SUM(preco * quantidade) AS total_gasto
    FROM vendas_carros
    GROUP BY cliente
""").show()
```
**Explicação:**  
- A função `SUM(preco * quantidade)` calcula o total gasto por cada cliente.  
- O `GROUP BY cliente` agrupa os valores por cliente, permitindo somar os gastos individuais.

---

### **d) Contar quantas vendas foram feitas por marca de automóvel**  
**Código PySpark:**  
```python
spark.sql("""
    SELECT marca, COUNT(*) AS total_vendas
    FROM vendas_carros
    GROUP BY marca
""").show()
```
**Explicação:**  
- `COUNT(*)` conta o número de vendas para cada marca.  
- `GROUP BY marca` agrupa os registos para contar separadamente cada marca.

---

### **e) Criar uma nova coluna chamada `valor_total` que representa o total gasto na compra (`preco * quantidade`)**  
**Código PySpark:**  
```python
spark.sql("""
    SELECT *, (preco * quantidade) AS valor_total
    FROM vendas_carros
""").show()
```
**Explicação:**  
- Criamos uma nova coluna `valor_total` calculada como `preco * quantidade`, representando o custo total de cada venda.  
- O uso de `SELECT *` mantém todas as colunas originais e adiciona a nova coluna calculada.

---

Este exercício ensina os alunos a **criar e manipular dados em PySpark**, incluindo **filtros, ordenação, agregação e cálculos matemáticos**.  
