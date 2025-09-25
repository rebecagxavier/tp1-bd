from datetime import datetime
import re
import psycopg2
from psycopg2 import sql
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT


conn = psycopg2.connect(dbname = "ecommerce", user = "postgres", password = "postgres", host = "db", port = "5432")
cursor = conn.cursor()

def consulta1(asin, cursor):
        #Dado um produto, listar os 5 comentários mais úteis e com maior 
        # avaliação e os 5 comentários mais úteis e com menor avaliação.
        query1 = f"SELECT id_cliente, nota, votos, util, data FROM avaliacao WHERE asin = '{asin}' ORDER BY util DESC, nota DESC LIMIT 5"
        cursor.execute(query1)
        lista_5_u_maior = cursor.fetchall()
        query2 = f"SELECT id_cliente, nota, votos, util, data FROM avaliacao WHERE asin = '{asin}' ORDER BY util DESC, nota ASC LIMIT 5"
        cursor.execute(query2)
        lista_5_u_menor = cursor.fetchall()

        print("5 comentários mais úteis e com maior avaliação: ")
        for tupla in lista_5_u_maior:
                id_cliente, nota, votos, util, data = tupla
                print(f"cliente: {id_cliente}\tnota: {nota}\tvotos: {votos}\tutil: {util}\tdata: {data}")
        print()

        print("5 comentários mais úteis e com menor avaliação: ")
        for tupla in lista_5_u_menor:
                id_cliente, nota, votos, util, data = tupla
                print(f"cliente: {id_cliente}\tnota: {nota}\tvotos: {votos}\tutil: {util}\tdata: {data}")

def consulta2(asin, cursor):
        #Dado um produto, listar os produtos similares com maiores vendas (melhor salesrank) do que ele.

        query1 = f'''
        SELECT produto.asin, produto.salesrank
        FROM produto JOIN
                (SELECT
                CASE
                        WHEN asin1 = '{asin}'  THEN asin2
                        ELSE asin1
                END AS asin
                FROM produto_similar
                WHERE asin1 = '{asin}'  OR asin2 = '{asin}'
                )as similares
        ON produto.asin = similares.asin
        WHERE produto.salesrank >
                (SELECT produto.salesrank 
                FROM produto
                WHERE produto.asin = '{asin}')
        ORDER BY produto.salesrank DESC
        '''
        cursor.execute(query1)
        similares = cursor.fetchall()
        print(f"Produtos similares ao produto {asin} com salesrank maior que ele:")
        if len(similares)>0:
                for similar in similares:
                        asin, salesrank = similar
                        print(f"asin: {asin}\tsalesrank: {salesrank}")
        else:
                print("Não tem")

def consulta3(asin, cursor):
        #Dado um produto, mostrar a evolução diária das médias 
        # de avaliação ao longo do período coberto no arquivo.

        print(f"Evolução diária de avaliações do produto: {asin}\n")
        query1  = f'''
                SELECT data, ROUND(avg(nota),2) 
                FROM avaliacao 
                WHERE asin = '{asin}'
                GROUP BY data
                '''
        cursor.execute(query1)
        grupo_data_media = cursor.fetchall()
        #print(grupo_data_media)
        i = 0
        for k, tupla in enumerate(grupo_data_media):
                data, nota = tupla
                i += nota
                print(f"{data}\t{round(i/(k+1),2)}")

def consulta4(cursor):
        query1 = '''
                WITH grupos AS (
                        SELECT asin, salesrank, grupo_produto,
                        ROW_NUMBER() OVER (PARTITION BY grupo_produto ORDER BY salesrank DESC) AS grupos
                        FROM Produto
                        )
                SELECT *
                FROM grupos
                WHERE grupos <= 10;
                '''
        cursor.execute(query1)
        lideres = cursor.fetchall()
        anterior = ""
        for lider in lideres:
                asin, salesrank, grupo, pos = lider
                if grupo != anterior:
                        anterior = grupo
                        print(f"\nGrupo: {grupo}")
                print(f"\t{pos:>2}: {asin}\tsalesrank: {salesrank}")

def consulta5(cursor):
        query1 = '''
                SELECT asin, ROUND(avg(util),2) as media_util
                FROM avaliacao
                WHERE avaliacao.nota >= 3
                GROUP BY asin
                ORDER BY media_util DESC
                LIMIT 10
                '''
        
        cursor.execute(query1)
        top_10 = cursor.fetchall()
        for top in top_10:
                asin, media = top
                print(f"{asin}\t{media}")

def consulta6(cursor):
        query1 = '''
        SELECT categoria.categoria, ROUND(AVG(media_util)) as media FROM 
	categoria JOIN
                (SELECT asin, ROUND(avg(util),2) as media_util 
                FROM avaliacao
                WHERE nota>=3
                GROUP BY asin
                ORDER BY media_util DESC) as avaliacao_medias
        ON categoria.asin = avaliacao_medias.asin
	GROUP BY categoria.categoria
	ORDER BY media DESC
        LIMIT 5
        '''
        cursor.execute(query1)
        top_5 = cursor.fetchall()
        for top in top_5:
                categoria, media = top
                print(f"{media}\t{categoria}")

def consulta7(cursor):
        #Listar os 10 clientes que mais fizeram comentários por grupo de produto.
        #vou em grupo ->


        query1 = '''
                WITH ranking AS (
                        SELECT 
                                COUNT(*) AS num_comentarios,
                                avaliacao.id_cliente,
                                produto.grupo_produto,
                                ROW_NUMBER() OVER (
                                PARTITION BY produto.grupo_produto 
                                ORDER BY COUNT(*) DESC
                                ) AS indice
                        FROM avaliacao
                        JOIN produto 
                                ON produto.asin = avaliacao.asin
                        GROUP BY avaliacao.id_cliente, produto.grupo_produto
                        )
                        SELECT * 
                        FROM ranking
                        WHERE indice <= 10;
        '''
        cursor.execute(query1)
        anterior = ""
        tabela_top_10 = cursor.fetchall()
        for top in tabela_top_10:
                num_comentarios, cliente_id, grupo, pos = top
                if grupo != anterior:
                        anterior = grupo
                        print(f"\nGrupo: {grupo}")
                print(f"{pos:>2}: {cliente_id:<15} comentários: {num_comentarios:,}")


asin = "B00000DFSQ"
consulta1(asin,cursor)
consulta2(asin,cursor)
consulta3(asin,cursor)
consulta4(cursor)
consulta5(cursor)
consulta6(cursor)
consulta7(cursor)
