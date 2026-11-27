-- Tabela da base analítica (executado na primeira criação do banco)

-- Uma linha por categoria em cada data de referência
CREATE TABLE IF NOT EXISTS metricas_categoria (
    data_referencia     DATE          NOT NULL,
    categoria           VARCHAR(100)  NOT NULL,
    preco_medio         NUMERIC(10, 2) NOT NULL,
    preco_minimo        NUMERIC(10, 2) NOT NULL,
    preco_maximo        NUMERIC(10, 2) NOT NULL,
    quantidade_produtos INTEGER       NOT NULL,
    dag_run_id          VARCHAR(250)  NOT NULL,
    atualizado_em       TIMESTAMPTZ   NOT NULL DEFAULT NOW(),
    PRIMARY KEY (data_referencia, categoria)
);
