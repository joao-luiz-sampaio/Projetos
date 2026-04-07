-- 1. Criando o comando para criar a tabela que receberá o seu arquivo listings.csv.

CREATE TABLE staging_airbnb (
    id BIGINT,
    name TEXT,
    host_id BIGINT,
    host_name VARCHAR(255),
    neighbourhood_group VARCHAR(255),
    neighbourhood VARCHAR(255),
    latitude NUMERIC,
    longitude NUMERIC,
    room_type VARCHAR(50),
    price NUMERIC, 
    minimum_nights INT,
    number_of_reviews INT,
    last_review DATE,
    reviews_per_month NUMERIC,
    calculated_host_listings_count INT,
    availability_365 INT,
    number_of_reviews_ltm INT,
    license TEXT
);

-- 2. Criando o Data Warehouse:

-- Criando as Dimensões
CREATE TABLE dim_localizacao (
    sk_localizacao SERIAL PRIMARY KEY,
    neighbourhood_group VARCHAR(255),
    neighbourhood VARCHAR(255),
    latitude NUMERIC,
    longitude NUMERIC
);

CREATE TABLE dim_host (
    sk_host SERIAL PRIMARY KEY,
    host_id BIGINT,
    host_name VARCHAR(255)
);

CREATE TABLE dim_imovel (
    sk_imovel SERIAL PRIMARY KEY,
    id_airbnb BIGINT,
    nome_anuncio TEXT,
    room_type VARCHAR(50),
    license TEXT
);

-- Criando a Fato
CREATE TABLE fato_anuncios (
    sk_imovel INT REFERENCES dim_imovel(sk_imovel),
    sk_localizacao INT REFERENCES dim_localizacao(sk_localizacao),
    sk_host INT REFERENCES dim_host(sk_host),
    preco_atual NUMERIC(10,2),
    total_reviews INT,
    data_ultima_review DATE,
    temporada_atividade VARCHAR(20)
);


-- 3. Comando de importação:
COPY staging_airbnb 
FROM 'C:\airbnb\listings.csv' 
WITH (FORMAT CSV, HEADER, DELIMITER ',', ENCODING 'UTF8');


-- 4. Inserindo nas dimensões: 

INSERT INTO dim_localizacao (neighbourhood_group, neighbourhood, latitude, longitude)
SELECT DISTINCT neighbourhood_group, neighbourhood, latitude, longitude FROM staging_airbnb;

INSERT INTO dim_host (host_id, host_name)
SELECT DISTINCT host_id, host_name FROM staging_airbnb;

INSERT INTO dim_imovel (id_airbnb, nome_anuncio, room_type, license)
SELECT DISTINCT id, name, room_type, license FROM staging_airbnb;

-- 5. Inserindo na Fato: 
INSERT INTO fato_anuncios (sk_imovel, sk_localizacao, sk_host, preco_atual, total_reviews, data_ultima_review, temporada_atividade)
SELECT 
    i.sk_imovel, l.sk_localizacao, h.sk_host, s.price, s.number_of_reviews, s.last_review,
    CASE 
        WHEN EXTRACT(MONTH FROM s.last_review) IN (12, 1, 2) THEN 'Verão'
        WHEN EXTRACT(MONTH FROM s.last_review) IN (3, 4, 5) THEN 'Outono'
        WHEN EXTRACT(MONTH FROM s.last_review) IN (6, 7, 8) THEN 'Inverno'
        WHEN EXTRACT(MONTH FROM s.last_review) IN (9, 10, 11) THEN 'Primavera'
        ELSE 'Sem Atividade'
    END
FROM staging_airbnb s
JOIN dim_imovel i ON s.id = i.id_airbnb
JOIN dim_host h ON s.host_id = h.host_id
JOIN dim_localizacao l ON s.latitude = l.latitude AND s.longitude = l.longitude;

-- 6. Desafio: Otimização de preço e sazonalidade. 
-- Analisar como a localização geográfica (bairros) e a sazonalidade(temporadas baseadas na última atividade) influenciam o preço das diárias, permitindo estratégias de precificação dinâmica.

SELECT 
    l.neighbourhood AS bairro,
    f.temporada_atividade,
    ROUND(AVG(f.preco_atual), 2) AS preco_medio,
    COUNT(*) AS quantidade_imoveis
FROM fato_anuncios f
JOIN dim_localizacao l ON f.sk_localizacao = l.sk_localizacao
WHERE f.temporada_atividade <> 'Sem Atividade'
GROUP BY l.neighbourhood, f.temporada_atividade
ORDER BY l.neighbourhood, preco_medio DESC;

-- 7. Resultado: Quadro resumo - Bairros mais baratos por estação: 

WITH ranking_precos AS (
    SELECT 
        l.neighbourhood AS bairro,
        f.temporada_atividade,
        ROUND(AVG(f.preco_atual), 2) AS preco_medio,
        -- Cria um ranking de preço (do menor para o maior) dentro de cada temporada
        RANK() OVER (PARTITION BY f.temporada_atividade ORDER BY AVG(f.preco_atual) ASC) AS posicao
    FROM 
        fato_anuncios f
    JOIN 
        dim_localizacao l ON f.sk_localizacao = l.sk_localizacao
    WHERE 
        f.temporada_atividade <> 'Inativo'
    GROUP BY 
        l.neighbourhood, f.temporada_atividade
)

SELECT 
    temporada_atividade,
    posicao,
    bairro,
    preco_medio
FROM 
    ranking_precos
WHERE 
    posicao <= 3 -- Filtra apenas os 3 mais baratos de cada estação
ORDER BY 
    temporada_atividade, posicao;
