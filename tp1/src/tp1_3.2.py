from datetime import datetime
import re
import psycopg2
from psycopg2 import sql
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT


def extrai_comentario(texto):
    items = re.split(r"\s+",texto)[1:]
    data = datetime.strptime(items[0], "%Y-%m-%d")
    id_cliente = items[2]
    nota = items[4]
    votos = items[6]
    util = items[8]
    return data, id_cliente, nota, votos, util
    
def cria_esquema(cursor):
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS produto (
            asin VARCHAR(60) PRIMARY KEY,
            id_produto INT UNIQUE,
            titulo_produto VARCHAR(500),
            grupo_produto VARCHAR(100),
            salesrank INT,
            simi_total INT
                );
                
        CREATE TABLE IF NOT EXISTS produto_similar(
            id_relacao SERIAL PRIMARY KEY,
            asin1 VARCHAR(60),
            asin2 VARCHAR(60),
            CONSTRAINT relacao_unica UNIQUE (asin1,asin2)
                );
        
        CREATE TABLE IF NOT EXISTS categoria(
            id_produto INT REFERENCES produto(id_produto),
            categoria VARCHAR(500)
                );

        CREATE TABLE IF NOT EXISTS cliente(
            id_cliente VARCHAR(60) PRIMARY KEY UNIQUE
                );

        CREATE TABLE IF NOT EXISTS avaliacoes_produto(
            id_avaliacao INT PRIMARY KEY,
            asin VARCHAR(60) REFERENCES produto(asin),
            total_ava INT,
            baixados INT,
            media FLOAT
                );
                   
        CREATE TABLE IF NOT EXISTS avaliacao(
            id_avaliacao INT REFERENCES avaliacoes_produto(id_avaliacao),
            id_cliente VARCHAR(60) REFERENCES cliente(id_cliente),
            nota INT,
            votos INT,
            util INT,
            data DATE
                );
    """)

    

def povoa(cursor, arquivo_entrada):
    
    with open(arquivo_entrada,"r", encoding="utf-8") as entrada:
        texto = entrada.read().split('Id',1)[-1].strip()
        produtos = re.findall(r"Id:\s+\d+\nASIN:.*?(?=\nId:\s+\d+\nASIN:|\Z)", texto, re.DOTALL)
        #produtos = texto.split("Id:")
    id_relacao = 0
    query_produto = "INSERT INTO produto (asin, id_produto, titulo_produto, grupo_produto, salesrank, simi_total)\nVALUES\n"
    produtos_sql = []
    
    query_clientes = "INSERT INTO cliente (id_cliente)\nVALUES\n"
    clientes_set = set()
    clientes_sql = []
   
    query_avaliacao_produto = "INSERT INTO avaliacoes_produto (id_avaliacao, asin, total_ava, baixados, media)\nVALUES\n"
    avaliacao_produto_sql = []
    query_avaliacao = "INSERT INTO avaliacao (id_avaliacao, id_cliente, nota, votos, util, data)\nVALUES\n"
    avaliacao_sql = []
    
    query_similares = "INSERT INTO produto_similar (asin1, asin2)\nVALUES\n"
    similares_sql = []

    query_categoria = "INSERT INTO categoria (id_produto, categoria)\nVALUES\n"
    categoria_sql = []
    for k, produto in enumerate(produtos):
        #if k>10:
            #break
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
        titulo = titulo.replace("'", "''")

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
        produtos_sql.append(f"('{asin}', {id},'{titulo}','{grupo_produto}',{salesrank},{simi_total})")
        avaliacao_produto_sql.append(f"({id}, '{asin}', {coms_total}, {coms_baixados}, {avg})")
    
        #insercao similares
        if similares is not None:
            for similar in similares:
                asin1, asin2 = sorted([asin,similar])
                similares_sql.append(f"('{asin1}', '{asin2}')")

        #insercao avaliacoes e clientes
        if comentarios is not None:
            for comentario in comentarios:
                data, id_cliente, nota, votos, util = extrai_comentario(comentario)
                #query_avaliacao += f"({id}, '{id_cliente}', {nota}, {votos}, {util}, '{data}'),\n"
                avaliacao_sql.append (f"({id}, '{id_cliente}', {nota}, {votos}, {util}, '{data}')")
                if id_cliente not in clientes_set:
                    clientes_set.add(id_cliente)
                    clientes_sql.append(f"('{id_cliente}')")

        #insercao categorias
        for categoria in categorias:
            categoria_sql.append(f"({id}, '{categoria}')")
        print(k)

    query_produto += ",\n".join(produtos_sql) + ";"
    query_avaliacao_produto += ",\n".join(avaliacao_produto_sql) + ";"
    query_clientes += ",\n".join(clientes_sql) + ";"
    query_similares += ",\n".join(similares_sql) + "\nON CONFLICT (asin1, asin2) DO NOTHING;\n"
    query_avaliacao += ",\n".join(avaliacao_sql) + ";"
    query_categoria += ",\n".join(categoria_sql) + ";"
    big_query = query_produto + "\n" + query_clientes + "\n" + query_categoria + "\n" + query_similares + query_avaliacao_produto + "\n" + query_avaliacao
    #print(query_categoria)
    #print(query_clientes)
    #print(query_categoria)
    #print(query_similares)
    #print(query_avaliacao_produto)
    #print(big_query)
    try:
        cursor.execute(big_query)
    except Exception as e:
        print(e)
    

#agora = datetime.now()
#print("Data e hora atual:", agora)
#agora = datetime.now()
#print("Data e hora atual:", agora) 


import argparse

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
        cria_esquema(cursor)
        povoa(cursor, args.input)
        print("oiiiiiii")
        conn.commit()
        conn.close()
        print("Esquema criado e dados populados com sucesso.")
        return 0
    except Exception as e:
        print("Erro:", e)
        return 1

        

if __name__ == "__main__":
    main()
