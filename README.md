# 1) Descompactar dataset
Acessar diretório "data" e descompactar arquivo "snap_amazon.zip"

# 2) Construir e subir os serviços
docker compose up -d --build

# 3) (Opcional) conferir saúde do PostgreSQL
docker compose ps

# 4) Criar esquema e carregar dados
docker compose run --rm app python src/tp1_3.2.py \
  --db-host db --db-port 5432 --db-name ecommerce --db-user postgres --db-pass postgres \
  --input /data/snap_amazon.txt

# 5) Executar o Dashboard (todas as consultas)
docker compose run --rm app python src/tp1_3.3.py \
  --db-host db --db-port 5432 --db-name ecommerce --db-user postgres --db-pass postgres \
  --output /app/out

