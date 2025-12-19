-- models/gold/dim_projetos_culturais.sql

{{ config(
    materialized='table',
    file_format='delta',
    alias='dim_projetos_culturais'
) }}

WITH latest_sat AS (
    -- Deduplicação usando lógica que antes estava no seu PySpark
    SELECT 
        hk_projeto,
        nome,
        CAST(valor_projeto AS DOUBLE) as valor_projeto,
        load_date,
        ROW_NUMBER() OVER (PARTITION BY hk_projeto ORDER BY load_date DESC) as rn
    FROM {{ source('silver', 'sat_projeto') }}
)

SELECT
    h.PRONAC as codigo_pronac,
    s.nome as nome_projeto,
    COALESCE(s.valor_projeto, 0.0) as valor_projeto,
    s.load_date as data_ultima_carga
FROM {{ source('silver', 'hub_projeto') }} h
JOIN latest_sat s ON h.hk_projeto = s.hk_projeto
WHERE s.rn = 1