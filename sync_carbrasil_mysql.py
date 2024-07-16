import pyodbc
from sync_bling_mysql import cursor, conn
import logging
#import arquivos_de_apoio.teste2 as t2

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
                "profundidade": product[16]
            }
            
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
        for dicts_db in database_response:
            for dicts_carbrasil in carbrasil_response:
                if dicts_db["codigo_carbrasil"] == dicts_carbrasil["codigo_carbrasil"]:

                    equal_description = dicts_db["descricao"] == dicts_carbrasil["descricao"]
                    equal_price = dicts_db["preco"] == dicts_carbrasil["preco"]
                    equal_cost = dicts_db["custo"] == dicts_carbrasil["custo"]
                    equal_stock = dicts_db["estoque"] == dicts_carbrasil["estoque"]

                    document = {
                        "codigo_carbrasil": dicts_db["codigo_carbrasil"],
                        "id_bling": dicts_db["id_bling"],
                        "id_estoque": dicts_db["id_estoque"],
                        "bling_tipo": dicts_db["bling_tipo"],
                        "bling_formato": dicts_db["bling_formato"],
                        "bling_situacao": dicts_db["bling_situacao"],
                        "gtin": dicts_db["gtin"],
                        "peso_liquido": dicts_db["peso_liquido"],
                        "peso_bruto": dicts_db["peso_bruto"],
                        "marca": dicts_db["marca"],
                        "largura": dicts_db["largura"],
                        "altura": dicts_db["altura"],
                        "profundidade": dicts_db["profundidade"],
                        "divergencias": {}
                    }

                    if not equal_description:
                        document["divergencias"]["descricao"] = dicts_carbrasil["descricao"]
                    if not equal_price:
                        document["divergencias"]["preco"] = dicts_carbrasil["preco"]
                    if not equal_cost:
                        document["divergencias"]["custo"] = dicts_carbrasil["custo"]
                    if not equal_stock:
                        document["divergencias"]["estoque"] = dicts_carbrasil["estoque"]

                    doc_keys = [x for x in document["divergencias"].keys()]
                    #Se todas as condições acima foram falsas, doc_keys só terá as chaves padrão, sinalizando que
                    # não há divergência nó código em questão
                    if not doc_keys:
                        break
                    
                    #Se tem "estoque" ou "custo" e não tem "preço" e "descrição", faz o append e sai do loop
                    if ("estoque" in doc_keys or "custo" in doc_keys) and ("preco" not in doc_keys and "descricao" not in doc_keys):
                        document["divergencias"].setdefault("estoque", dicts_carbrasil["estoque"])
                        document["divergencias"].setdefault("custo", dicts_carbrasil["custo"]) 
                        document["divergencias"].setdefault("preco", dicts_carbrasil["preco"])
                        diagnosis.append(document)
                        break
                    
                    #adiciona a descrição do produto no dicionário, pois a api de produtos pede como obrigatório.
                    #Neste ponto só chegaria os produtos que tem divergência na descrição ou no preço.
                    document["divergencias"].setdefault("descricao", dicts_carbrasil["descricao"])
                    document["divergencias"].setdefault("preco", dicts_carbrasil["preco"])
                    diagnosis.append(document)
                    break

        print("(compare_responses) Diagnóstico completo")
        logger.info("(compare_responses) Diagnóstico completo")
        return diagnosis
    
    def update_custo_estoque_mysql(self, diagnosis):
        print("(update_custo_estoque_mysql) Atualizando custo e estoque")
        for product in diagnosis:
            if "custo" in product["divergencias"].keys() and "estoque" in product["divergencias"].keys():
                cursor.execute(f"UPDATE db_sistema_intermediador SET custo = {product['divergencias']['custo']}, estoque = {int(product['divergencias']['estoque'])} WHERE codigo_carbrasil = {product['codigo_carbrasil']} ")
        conn.commit()
        print("(update_custo_estoque_mysql) Atualização concluída")

    def loop(self):
        db_products = self.database_get_all()
        #db_products = t2.database_response

        carbrasil_products = self.carbrasil_database_get(db_products)
        #carbrasil_products = t2.carbrasil_response
        diagnosis = self.compare_responses(carbrasil_response=carbrasil_products, database_response=db_products)
        self.update_custo_estoque_mysql(diagnosis)
        return diagnosis
    

