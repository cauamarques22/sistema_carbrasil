#Import libraries
import mysql.connector
import requests
import json
import time
import logging

#Import modules
import auth_routine

logger = logging.getLogger(__name__)
logging.basicConfig(filename="app_logs.log", encoding="utf-8", level=logging.DEBUG, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

#Se conecta ou cria o banco de dados do sistema.
try:
    conn = mysql.connector.connect(user="root", password="123456", database="intermediador_bling", host="localhost") 
    cursor = conn.cursor(buffered=True)
    cursor.execute("SELECT * FROM db_sistema_intermediador")
except mysql.connector.ProgrammingError as err:
    if err.errno == 1049:
        conn = mysql.connector.connect(user="root", password="123456", host="localhost")
        cursor = conn.cursor()
        cursor.execute("CREATE DATABASE intermediador_bling")
        cursor.execute("USE intermediador_bling")
        cursor.execute("CREATE TABLE db_sistema_intermediador(codigo_carbrasil INT PRIMARY KEY, id_bling BIGINT, idEstoque BIGINT,\
                       descricao VARCHAR(255), preco FLOAT, custo FLOAT, estoque INT, bling_tipo VARCHAR(1), bling_formato VARCHAR(1), \
                       bling_situacao VARCHAR(1), gtin VARCHAR(50), peso_liquido FLOAT, peso_bruto FLOAT, marca VARCHAR(255), largura FLOAT, \
                       altura FLOAT, profundidade FLOAT)")
        conn.commit()
    if err.errno == 1146:
        cursor.execute("CREATE TABLE db_sistema_intermediador(codigo_carbrasil INT PRIMARY KEY, id_bling BIGINT, idEstoque BIGINT,\
                       descricao VARCHAR(255), preco FLOAT, custo FLOAT, estoque INT, bling_tipo VARCHAR(1), bling_formato VARCHAR(1), \
                       bling_situacao VARCHAR(1), gtin VARCHAR(50), peso_liquido FLOAT, peso_bruto FLOAT, marca VARCHAR(255), largura FLOAT, \
                       altura FLOAT, profundidade FLOAT)")
        conn.commit()

class BlingDatabaseSync():
    def __init__(self):
        self.bling_products = []

    def api_calls_get(self):
        counter = 1
        products_per_page = []
        all_products = []
        print("(api_calls_get) Solicitando produtos ao Bling..")
        while True:
            time.sleep(1.1)
            headers = {
                "Authorization": f"Bearer {auth_routine.session_tokens[0]}"
            }

            payload = {
                "limite": 500,
                "pagina": counter,
                "criterio": 2
            }

            r = requests.get(f"{auth_routine.HOST}produtos", params=payload, headers=headers)
            parsed = json.loads(r.text)

            if not parsed.get("data"): #exception KeyError
                break

            products_per_page.append(parsed["data"])
            counter +=1
        
        for x in products_per_page:
            for prod in x:
                all_products.append(prod)
        print("(api_calls_get) Produtos recebidos com sucesso.")
        
        self.bling_products = all_products

    def update_database(self):
        print("(update_database) Iniciando atualização do Banco de Dados.")
        for item in self.bling_products:
            try:
                cursor.execute(f"SELECT * FROM db_sistema_intermediador WHERE codigo_carbrasil = {int(item['codigo'])}")
                response = cursor.fetchall()
                if not response:
                    cursor.execute(f"""INSERT INTO db_sistema_intermediador \
                                (codigo_carbrasil, id_bling, descricao, preco, bling_tipo, bling_formato, bling_situacao) \
                                VALUES ({int(item['codigo'])}, {item['id']}, '{item['nome']}', {item['preco']}, '{item['tipo']}', '{item['formato']}', '{item['situacao']}')""")
            except ValueError as err:
                logger.error(f"(update_database) Produtos retornaram com erros: \n{item}")
                logger.error(err)
                continue
        conn.commit()
        print("(update_database) Atualização Concluída.")

    def bling_routine(self):
        while True:
            self.api_calls_get()
            self.update_database()
            time.sleep(30 * 60)


