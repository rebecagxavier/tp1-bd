# 1) Baixar e preparar o dataset
Acesse o site https://snap.stanford.edu/data/amazon-meta.html, baixe o arquivo, descompacte e renomeie para snap_amazon.txt. Em seguida, coloque o arquivo no diretório 'data' do projeto.

# 2) Construir e subir os serviços
docker compose up -d --build

# 3) (Opcional) conferir saúde do PostgreSQL
docker compose ps

# 4) Criar esquema e carregar dados
docker compose run --rm app python src/tp1_3.2.py --db-host db --db-port 5432 --db-name ecommerce --db-user postgres --db-pass postgres --input /data/snap_amazon.txt

# 5) Executar o Dashboard (todas as consultas)
docker compose run --rm app python src/tp1_3.3.py --db-host db --db-port 5432 --db-name ecommerce --db-user postgres --db-pass postgres --product-asin 0060094818 --output /app/out



