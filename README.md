# Pipeline de preços por categoria

DAG do Airflow que busca os produtos na [FakeStore API](https://fakestoreapi.com/docs),
calcula preço médio, mínimo, máximo e quantidade por categoria e grava no
PostgreSQL. Roda todo dia às 06:00 (horário de Brasília).

## Como executar

```bash
docker compose up -d
```

No Linux, antes: `mkdir -p logs && chmod 777 logs`.

Acesse http://localhost:8080 (admin / admin), ative a DAG
`precos_por_categoria` e clique em Trigger DAG.

Ver o resultado:

```bash
docker compose exec postgres-analytics psql -U analytics -d shopbrasil -c "SELECT * FROM metricas_categoria"
```

## Fluxo

1. `ingestao`: busca os produtos na API.
2. `analise`: agrupa por categoria, calcula as métricas com `.expand()` (pool
   `ecommerce_pool`, 2 slots) e consolida.
3. `persistencia`: grava no banco.

A tabela `metricas_categoria` não duplica linhas: a task apaga os registros da
data e insere de novo na mesma transação.

Como a API só devolve o catálogo atual, reprocessar uma data antiga grava os
preços de hoje.

## Padrões do time

- TaskFlow API e uma responsabilidade por task.
- Credenciais em Connections.
- Gravações que possam ser reexecutadas sem duplicar dados.
- Chamadas externas com timeout, retry e `raise` no erro.
