import pyodbc
import logging
import time

#APP modules
from sync_bling_mysql import cursor, conn

logger = logging.getLogger(__name__)
logging.basicConfig(filename="app_logs.log", encoding="utf-8", level=logging.DEBUG , format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

class DatabaseSync():
    def database_get_all(self) -> list[dict]:
        print("(database_get_all) Solicitando produtos ao Banco de Dados Interno")
        logger.info("(database_get_all) Solicitando produtos ao Banco de Dados Interno")
        cursor.execute("SELECT * FROM db_sistema_intermediador")
        resp = cursor.fetchall()

        all_database_products = []
        for product in resp:
            document = {
                "codigo_carbrasil": product[0],
                "id_bling": product[1],
                "id_estoque": product[2],
                "descricao": product[3],
                "preco": product[4],
                "custo": product[5],
                "estoque" :product[6],
                "bling_tipo": product[7],
                "bling_formato": product[8],
                "bling_situacao": product[9],
                "gtin": product[10],
                "peso_liquido": product[11],
                "peso_bruto": product[12],
                "marca": product[13],
                "largura": product[14],
                "altura": product[15],
                "profundidade": product[16],
                "internal_error_count": product[17],
                "ignore_code": product[18]
            }
            if document["ignore_code"] == 1:
                continue
            if document["internal_error_count"] > 0:
                continue

            all_database_products.append(document)
        print("(database_get_all) Produtos obtidos com sucesso.")
        logger.info("(database_get_all) Produtos obtidos com sucesso.")
        return all_database_products

    def carbrasil_database_get(self, products)-> list[dict]:
        print("(carbrasil_database_get) Se conectando ao banco de dados da Car Brasil.")
        logger.info("(carbrasil_database_get) Se conectando ao banco de dados da Car Brasil.")
        cnxn_str = ("Driver={SQL SERVER};"
                "Server=CARBRASIL-HOST\\SQLEXPRESS;"
                "Database=bdados1;"
                "UID=sa;"
                "PWD=Tec12345678;")

        cnxn = pyodbc.connect(cnxn_str)
        cb_cursor = cnxn.cursor()

        carbrasil_responses = []
        print("(carbrasil_database_get) Solicitando produtos ao Banco de Dados CarBrasil")
        logger.info("(carbrasil_database_get) Solicitando produtos ao Banco de Dados CarBrasil")
        for product in products:
            response = cb_cursor.execute(f"SELECT codprod, descricao, eatu, pvenda, custo_base FROM v_produtos1 WHERE ativa=0 AND codprod={product['codigo_carbrasil']}")
            for x in response:
                document = {
                    "codigo_carbrasil": x[0],
                    "descricao": x[1].strip(),
                    "preco": float(x[3]),
                    "custo": float(x[4]),
                    "estoque": float(x[2])
                }
                carbrasil_responses.append(document)

        print("(carbrasil_database_get) Produtos obtidos com sucesso")
        logger.info("(carbrasil_database_get) Produtos obtidos com sucesso")
        return carbrasil_responses

    def compare_responses(self, carbrasil_response: list[dict], database_response: list[dict]) -> list[dict]:
        print("(compare_responses) Comparando respostas dos dois Bancos de Dados e montando diagnóstico")
        logger.info("(compare_responses) Comparando respostas dos dois Bancos de Dados e montando diagnóstico")
        
        diagnosis = []
        for dict_mysql in database_response:
            for dict_carbrasil in carbrasil_response:
                if dict_mysql["codigo_carbrasil"] == dict_carbrasil["codigo_carbrasil"]:

                    #Criação do Dicionário com as informações do produto, e as divergências.
                    #Nas divergências estarão os valores reais, que devem ser atualizados.
                    document = dict_mysql.copy()
                    del document["descricao"]
                    del document["preco"]
                    del document["custo"]
                    del document["estoque"]
                    document["divergencias"] = {}

                    #Comparação
                    equal_description = dict_mysql["descricao"] == dict_carbrasil["descricao"]
                    equal_price = dict_mysql["preco"] == dict_carbrasil["preco"]
                    equal_cost = dict_mysql["custo"] == dict_carbrasil["custo"]
                    equal_stock = dict_mysql["estoque"] == dict_carbrasil["estoque"]

                    if not equal_description:
                        document["divergencias"]["descricao"] = dict_carbrasil["descricao"]
                    if not equal_price:
                        document["divergencias"]["preco"] = dict_carbrasil["preco"]
                    if not equal_cost:
                        document["divergencias"]["custo"] = dict_carbrasil["custo"]
                    if not equal_stock:
                        document["divergencias"]["estoque"] = dict_carbrasil["estoque"]

                    doc_keys = [x for x in document["divergencias"].keys()]
                    #Se todas as condições acima foram falsas, doc_keys só terá as chaves padrão, sinalizando que
                    #não há divergência nó código em questão
                    if not doc_keys:
                        break

                    #Se tem ("estoque" ou "custo") E (não tem "preço" e "descrição") nas chaves do dicionário, faz o append e sai do loop
                    #Será redirecionado para API estoque
                    if ("estoque" in doc_keys or "custo" in doc_keys) and ("preco" not in doc_keys and "descricao" not in doc_keys):
                        document["divergencias"].setdefault("estoque", dict_carbrasil["estoque"])
                        document["divergencias"].setdefault("custo", dict_carbrasil["custo"]) 
                        document["divergencias"].setdefault("preco", dict_carbrasil["preco"])
                        document["endpoint_correto"] = "estoque"
                        diagnosis.append(document)
                        break

                    #Se tem ("descrição" ou "preco") E (não tem "estoque" e "custo") nas chaves do dicionário, faz o append e sai do loop
                    #Será redirecionado para API produto 
                    if ("descricao" in doc_keys or "preco" in doc_keys) and ("estoque" not in doc_keys and "custo" not in doc_keys):
                        document["divergencias"].setdefault("descricao", dict_carbrasil["descricao"])
                        document["divergencias"].setdefault("preco", dict_carbrasil["preco"])
                        document["endpoint_correto"] = "produto"
                        diagnosis.append(document)
                        break

                    #Caso nenhuma condição sirva, o dicionário passará por duas APIs, sendo necessário colocar essas informações por padrão
                    document["divergencias"].setdefault("descricao", dict_carbrasil["descricao"])
                    document["divergencias"].setdefault("preco", dict_carbrasil["preco"])
                    document["divergencias"].setdefault("estoque", dict_carbrasil["estoque"])
                    document["divergencias"].setdefault("custo", dict_carbrasil["custo"])
                    document["endpoint_correto"] = "ambos"
                    diagnosis.append(document)
                    break

        print("(compare_responses) Diagnóstico completo")
        logger.info("(compare_responses) Diagnóstico completo")
        return diagnosis
    
    @classmethod
    def update_mysql_db(self, diagnosis):
        print("(update_mysql_db) Atualizando custo e estoque")
        logger.info("(update_mysql_db) Atualizando custo e estoque")
        for product in diagnosis:
            prod_keys = [x for x in product["divergencias"].keys()]
            if "custo" in prod_keys:
                cursor.execute("UPDATE db_sistema_intermediador SET custo = %s WHERE codigo_carbrasil = %s", (product['divergencias']['custo'], product['codigo_carbrasil']))
            if "estoque" in prod_keys:
                cursor.execute("UPDATE db_sistema_intermediador SET estoque = %s WHERE codigo_carbrasil = %s", (product['divergencias']['estoque'], product['codigo_carbrasil']))
            if "preco" in prod_keys:
                cursor.execute("UPDATE db_sistema_intermediador SET preco = %s WHERE codigo_carbrasil = %s", (product['divergencias']['preco'], product['codigo_carbrasil']))
            if "descricao" in prod_keys:
                cursor.execute("UPDATE db_sistema_intermediador SET descricao = %s WHERE codigo_carbrasil = %s", (product['divergencias']['descricao'], product['codigo_carbrasil']))
        conn.commit()
        print("(update_mysql_db) Atualização concluída")
        logger.info("(update_mysql_db) Atualização concluída")

    @classmethod
    def reset_internal_error_count(self):
        time.sleep(1*60*60)
        cursor.execute("UPDATE db_sistema_intermediador SET internal_error_count = 0 WHERE internal_error_count > 0")
        conn.commit()

    def main(self):
        db_products = self.database_get_all()
        carbrasil_products = self.carbrasil_database_get(db_products)
        diagnosis = self.compare_responses(carbrasil_response=carbrasil_products, database_response=db_products)
        logger.info(f"Tamanho da lista 'diagnosis': {len(diagnosis)}")
        #self.update_mysql_db(diagnosis)
        return diagnosis
    

