from datetime import datetime
import io
import re
import psycopg2
from psycopg2 import sql
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import argparse


def extrai_comentario(texto):
    items = re.split(r"\s+",texto)[1:]
    data = datetime.strptime(items[0], "%Y-%m-%d")
    id_cliente = items[2]
    nota = items[4]
    votos = items[6]
    util = items[8]
    return data, id_cliente, nota, votos, util
    
def cria_esquema(conn, cursor):
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS produto (
            asin VARCHAR(60) PRIMARY KEY UNIQUE,
            id_produto INT UNIQUE,
            titulo_produto VARCHAR(500),
            grupo_produto VARCHAR(100),
            salesrank INT,
            simi_total INT
                );
                
        CREATE TABLE IF NOT EXISTS produto_similar (
            id_relacao SERIAL PRIMARY KEY,
            asin1 VARCHAR(60) REFERENCES produto(asin) NOT NULL,
            asin2 VARCHAR(60) REFERENCES produto(asin) NOT NULL
        );
        
        CREATE TABLE IF NOT EXISTS categoria (
            id_categoria SERIAL PRIMARY KEY,
            asin VARCHAR(60) REFERENCES produto(asin) NOT NULL,
            categoria VARCHAR(500)
        );


        CREATE TABLE IF NOT EXISTS avaliacoes_produto (
            id_avaliacao_produto SERIAL PRIMARY KEY,
            asin VARCHAR(60) REFERENCES produto(asin) NOT NULL,
            total_ava INT,
            baixados INT,
            media FLOAT
        );

        CREATE TABLE IF NOT EXISTS avaliacao (
            id_avaliacao SERIAL PRIMARY KEY,
            asin VARCHAR(60) REFERENCES produto(asin) NOT NULL,
            id_cliente VARCHAR(60),
            nota INT,
            votos INT,
            util INT,
            data DATE
        );
        CREATE OR REPLACE FUNCTION ignorar_similares_invalidos()
        RETURNS TRIGGER AS $$
        BEGIN
            IF EXISTS (SELECT 1 FROM produto WHERE asin = NEW.asin1)
            AND EXISTS (SELECT 1 FROM produto WHERE asin = NEW.asin2) THEN
                RETURN NEW; -- permite a inserção
            ELSE
                RETURN NULL; -- ignora a inserção
            END IF;
        END;
        $$ LANGUAGE plpgsql;

        CREATE TRIGGER validar_similares
        BEFORE INSERT ON produto_similar
        FOR EACH ROW
        EXECUTE FUNCTION ignorar_similares_invalidos();
    """)
    conn.commit()


    
def sanitize(value):
    if value is None or str(value).strip() == "":
        return "\\N" 
    return str(value).replace("\t", " ").replace("\n", " ").replace("\\", "")



def povoa(conn, cursor, arquivo_entrada):
    print("Lendo arquivo de entrada...")
    with open(arquivo_entrada,"r", encoding="utf-8") as entrada:
        texto = entrada.read().split('Id',1)[-1].strip()
        produtos = re.findall(r"Id:\s+\d+\nASIN:.*?(?=\nId:\s+\d+\nASIN:|\Z)", texto, re.DOTALL)
    print("Arquivo de entrada lido")

    print("Colentando dados...")
    id_relacao = 0
    buffer_produto = io.StringIO()
    query_produto = "INSERT INTO produto (asin, id_produto, titulo_produto, grupo_produto, salesrank, simi_total)\nVALUES\n"
    produtos_sql = []
   
    buffer_avaliacao_produto = io.StringIO()
    query_avaliacao_produto = "INSERT INTO avaliacoes_produto (asin, total_ava, baixados, media)\nVALUES\n"
    avaliacao_produto_sql = []

    buffer_avaliacao = io.StringIO()
    query_avaliacao = "INSERT INTO avaliacao (asin, id_cliente, nota, votos, util, data)\nVALUES\n"
    avaliacao_sql = []
    
    buffer_similares = io.StringIO()
    query_similares = "INSERT INTO produto_similar (asin1, asin2)\nVALUES\n"
    similares_sql = []

    buffer_categoria = io.StringIO()
    query_categoria = "INSERT INTO categoria (asin, categoria)\nVALUES\n"
    categoria_sql = []
    for k, produto in enumerate(produtos):
        id = 0
        asin = ""
        titulo = ""
        grupo_produto = ""
        salesrank = 0
        simi_total = 0
        coms_total = 0
        coms_baixados = 0
        avg = 0
        
        categorias = []
        if "discontinued product" in produto:
            continue

        linhas = produto.split("\n")
        id = int(linhas[0].split("Id:")[1])
        try:
            linha_similares = re.split(r"similar:|\s+", linhas[5])
            similares = linha_similares[4:]
        except Exception as e:
            print(f"Erro ao obter similares id: {id}. Assumindo valor nulo")
            similares = None
        try:
            comentarios = (produto.split("reviews: ")[1]).split("\n")[1:-2]
        except Exception as e:
            comentarios = None
            print(f"Erro ao obter comentarios id: {id}. Assumindo 0")
        asin = linhas[1].split("ASIN: ")[1]
        titulo = linhas[2].split("title:")[1]

        try:
            grupo_produto = linhas[3].split("group:")[1]        
        except Exception as e:
            print(f"Erro ao obter o grupo id: {id}. Assumindo valor null")
            grupo_produto = None
        try:
            salesrank = int(linhas[4].split("salesrank:")[1])
        except Exception as e:
            print(linhas)
            print(f"Erro ao obter salesrank id: {id}. Assumindo valor -1")
            salesrank = -1
        simi_total = int(linha_similares[3])
            
        coms_total = int(re.findall(r"total: (\d+)", produto)[0])
        coms_baixados = int(re.findall(r"downloaded: (\d+)", produto)[0])
        avg = float(re.findall(r"avg rating: (\d+(?:\.\d+)?)", produto)[0])

        num_categorias = int(linhas[6].split("categories:")[1])
        for i in range(1,num_categorias+1):
            categorias.append(linhas[6+i].lstrip().replace("'","''"))

        #insercao produtos e avaliacao_produtos
        buffer_produto.write(f"{sanitize(asin)}\t{sanitize(id)}\t{sanitize(titulo)}\t{sanitize(grupo_produto)}\t{sanitize(salesrank)}\t{sanitize(simi_total)}\n")
        buffer_avaliacao_produto.write(f"{asin}\t{coms_total}\t{coms_baixados}\t{avg}\n")
    
        #insercao similares
        if similares is not None:
            for similar in similares:
                asin1, asin2 = sorted([asin,similar])
                buffer_similares.write(f"{asin1}\t{asin2}\n")

        #insercao avaliacoes
        if comentarios is not None:
            for comentario in comentarios:
                data, id_cliente, nota, votos, util = extrai_comentario(comentario)
                buffer_avaliacao.write(f"{asin}\t{id_cliente}\t{nota}\t{votos}\t{util}\t{data}\n")
        
        #insercao categorias
        for categoria in categorias:
            buffer_categoria.write(f"{asin}\t{categoria}\n")

    buffer_produto.seek(0)
    buffer_avaliacao_produto.seek(0)
    buffer_avaliacao.seek(0)
    buffer_similares.seek(0)
    buffer_categoria.seek(0)
    print("Dados coletados")
    try:
        print("Povoando tabela produto...")
        cursor.copy_from(
            buffer_produto,
            "produto",
            columns=("asin", "id_produto", "titulo_produto", "grupo_produto", "salesrank", "simi_total"),
            sep="\t"
        )
        conn.commit()
        print("Tabela produtos povoada.")

        print("Povoando tabela avaliacoes_produto...")
        cursor.copy_from(
            buffer_avaliacao_produto,
            "avaliacoes_produto",
            columns=("asin", "total_ava", "baixados", "media"),
            sep="\t"
        )
        conn.commit()
        print("Tabela avaliacoes_produto")

        print("Povoando tabela avaliacao...")
        cursor.copy_from(
            buffer_avaliacao,
            "avaliacao",
            columns=("asin", "id_cliente", "nota", "util", "votos", "data"),
            sep="\t"
        )
        conn.commit()
        print("Tabela avaliacao povoada...")

        print("Povoando tabela produto_similar...")
        cursor.copy_from(
            buffer_similares,
            "produto_similar",
            columns=("asin1", "asin2"),
            sep="\t"
        )
        conn.commit()
        print("Tabela produto_similar povoada")

        print("Povoando tabela categoria...")
        cursor.copy_from(
            buffer_categoria,
            "categoria",
            columns=("asin", "categoria"),
            sep="\t"
        )
        conn.commit()
        print("Tabela categoria povoada")

    except Exception as e:
        print(e)
    

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--db-host", required=True)
    parser.add_argument("--db-port", required=True)
    parser.add_argument("--db-name", required=True)
    parser.add_argument("--db-user", required=True)
    parser.add_argument("--db-pass", required=True)
    parser.add_argument("--input", required=True)
    args = parser.parse_args()

    try:
        conn = psycopg2.connect(
            dbname=args.db_name,
            user=args.db_user,
            password=args.db_pass,
            host=args.db_host,
            port=args.db_port
        )
        cursor = conn.cursor()
        cria_esquema(conn,cursor)
        povoa(conn,cursor, args.input)
        conn.close()
        print("Esquema criado e dados populados com sucesso.")
        return 0
    except Exception as e:
        print("Erro:", e)
        return 1

        

if __name__ == "__main__":
    main()
