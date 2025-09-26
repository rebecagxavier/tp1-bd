import argparse
from datetime import datetime
import re
import psycopg2
from psycopg2 import sql
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT




def consulta1(asin, cursor):
        #Dado um produto, listar os 5 comentários mais úteis e com maior 
        # avaliação e os 5 comentários mais úteis e com menor avaliação.
        query1 = f"SELECT id_cliente, nota, votos, util, data FROM avaliacao WHERE asin = '{asin}' ORDER BY util DESC, nota DESC LIMIT 5"
        cursor.execute(query1)
        lista_5_u_maior = cursor.fetchall()
        query2 = f"SELECT id_cliente, nota, votos, util, data FROM avaliacao WHERE asin = '{asin}' ORDER BY util DESC, nota ASC LIMIT 5"
        cursor.execute(query2)
        lista_5_u_menor = cursor.fetchall()

        print("\t5 comentários mais úteis e com maior avaliação:\n")
        print(f"\t{'cliente':<20}{'nota':<6}{'votos':<8}{'util':<6}{'data'}")
        for tupla in lista_5_u_maior:
                id_cliente, nota, votos, util, data = tupla
                print(f"\t{id_cliente:<20}{nota:<6}{votos:<8}{util:<6}{data}")
        print()

        print("\t5 comentários mais úteis e com menor avaliação:\n")
        print(f"\t{'cliente':<20}{'nota':<6}{'votos':<8}{'util':<6}{'data'}")
        for tupla in lista_5_u_menor:
                id_cliente, nota, votos, util, data = tupla
                print(f"\t{id_cliente:<20}{nota:<6}{votos:<8}{util:<6}{data}")
        print()

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
        print(f"\tProdutos similares ao produto {asin} com salesrank maior que ele:\n")
        print(f"\t{'asin':<16}{'salesrank':<10}")
        if len(similares)>0:
                for similar in similares:
                        asin, salesrank = similar
                        print(f"\t{asin:<15}\t{salesrank}")
        else:
                print("\tNão tem")
        print()

def consulta3(asin, cursor):
        #Dado um produto, mostrar a evolução diária das médias 
        # de avaliação ao longo do período coberto no arquivo.
        query1  = f'''
                SELECT data, ROUND(avg(nota),2) 
                FROM avaliacao 
                WHERE asin = '{asin}'
                GROUP BY data
                '''
        cursor.execute(query1)
        grupo_data_media = cursor.fetchall()
        print(f"\tEvolução diária das médias de avaliação do produto: {asin}\n")
        #print(grupo_data_media)
        i = 0
        print(f"\t{'data':<16}{'media'}")
        for k, tupla in enumerate(grupo_data_media):
                data, nota = tupla
                i += nota
                print(f"\t{data}\t{round(i/(k+1),2)}")
        print()

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
        print("\tProdutos líderes de venda por grupo de produto:")
        for lider in lideres:
                asin, salesrank, grupo, pos = lider
                if grupo != anterior:
                        anterior = grupo
                        print(f"\n\tGrupo: {grupo}\n")
                        print(f"\t{'pos':<7} {'asin':<16}{'salesrank'}")
                print(f"\t{pos:<8}{asin}\t{salesrank}")
        print()

def consulta5(cursor):
        query1 = '''
                SELECT asin, ROUND(avg(util),2) as media_util
                FROM avaliacao
                WHERE avaliacao.nota >= 4
                GROUP BY asin
                ORDER BY media_util DESC
                LIMIT 10
                '''
        
        cursor.execute(query1)
        top_10 = cursor.fetchall()
        print("\tTop 10 Produtos com maior média de avaliações positivas úteis por produto:\n")
        print(f"\t{'pos':<8}{'asin':<9}\tmedia")
        for i,top in enumerate(top_10):
                asin, media = top
                print(f"\t{i+1:<8}{asin}\t{media}")
        print()

def consulta6(cursor):
        query1 = '''
        SELECT categoria.categoria, ROUND(AVG(media_util)) as media FROM 
	categoria JOIN
                (SELECT asin, ROUND(avg(util),2) as media_util 
                FROM avaliacao
                WHERE nota>=4
                GROUP BY asin
                ORDER BY media_util DESC) as avaliacao_medias
        ON categoria.asin = avaliacao_medias.asin
	GROUP BY categoria.categoria
	ORDER BY media DESC
        LIMIT 5
        '''
        cursor.execute(query1)
        top_5 = cursor.fetchall()
        print("\tTop 5 Categorias com maior média de avaliação útil positiva por produto:\n")
        print(f"\t{'pos':<8}{'media':8}{'categoria'}")
        for i,top in enumerate(top_5):
                categoria, media = top
                print(f"\t{i+1:<5}\t{media}\t{categoria}")
        print()

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
        print("\n\tTop 10 clientes que mais fizeram comentários por grupo de produto:")
        tabela_top_10 = cursor.fetchall()
        for top in tabela_top_10:
                num_comentarios, cliente_id, grupo, pos = top
                if grupo != anterior:
                        anterior = grupo
                        print(f"\n\tGrupo: {grupo}\n")
                        print(f"\t{'pos':<8}{'cliente':<9}\tcomentarios")

                print(f"\t{pos:<7} {cliente_id:<15} {num_comentarios:,}")
        print()



def main():
        parser = argparse.ArgumentParser()
        parser.add_argument("--db-host", required=True)
        parser.add_argument("--db-port", required=True)
        parser.add_argument("--db-name", required=True)
        parser.add_argument("--db-user", required=True)
        parser.add_argument("--db-pass", required=True)
        parser.add_argument("--product-asin", required=True)
        parser.add_argument("--output", required=True)
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
                asin = args.product_asin
                print("Consulta 1\n")
                consulta1(asin,cursor)
                print("Consulta 2\n")
                consulta2(asin,cursor)
                print("Consulta 3\n")
                consulta3(asin,cursor)
                print("Consulta 4\n")
                consulta4(cursor)
                print("Consulta 5\n")
                consulta5(cursor)
                print("Consulta 6\n")
                consulta6(cursor)
                print("Consulta 7\n")
                consulta7(cursor)
                return 0
        except Exception as e:
                print(f"Erro: {e}")
                return 1
if __name__ == "__main__":
    main()