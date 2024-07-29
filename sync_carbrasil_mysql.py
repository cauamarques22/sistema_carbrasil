import logging
import time
import datetime

logging.basicConfig(filename="app_logs.log", encoding="utf-8", level=logging.DEBUG , format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger("sync_carbrasil_mysql")

class DatabaseSync():
    def __init__(self, UI, pause_event=None, stop_event=None):
        super().__init__()
        self.txbox = UI.modulo3_textbox
        self._pause_trigger = pause_event
        self._stop_trigger = stop_event
        self.UI = UI
        
        #Database Connections
        self._internal_error_conn = None
        self._internal_error_cursor = None
        self.conn = None
        self.cursor = None
        self._cb_conn = None
        self._cb_cursor = None

    @property
    def internal_error_conn(self):
        return self._internal_error_conn
    
    @internal_error_conn.setter
    def internal_error_conn(self, error_conn):
        self._internal_error_conn = error_conn
        self._internal_error_cursor = self._internal_error_conn.cursor()
    
    @property
    def general_db_conn(self):
        return self.conn
    
    @general_db_conn.setter
    def general_db_conn(self, conn):
        self.conn = conn
        self.cursor = self.conn.cursor()
    
    @property
    def cb_conn(self):
        return self._cb_conn
    
    @cb_conn.setter
    def cb_conn(self, conn):
        self._cb_conn = conn
        self._cb_cursor = self._cb_conn.cursor()

    def displayer(self, msg):
        print(msg)
        time = datetime.datetime.now().strftime("%d/%m/%Y %H:%M:%S")
        self.txbox.insert('end', f"{time} - {msg}\n")
        logger.info(msg)

    def database_get_all(self) -> list[dict]:
        self.displayer("(database_get_all) Solicitando produtos ao Banco de Dados Interno")
        self.cursor.execute("SELECT * FROM db_sistema_intermediador")
        resp = self.cursor.fetchall()

        all_database_products = []
        for product in resp:
            document = {
                "codigo_carbrasil": product[0],
                "id_bling": product[1],
                "id_estoque": product[2],
                "bling_tipo": product[7],
                "bling_formato": product[8],
                "bling_situacao": product[9],
                "internal_error_count": product[17],
                "ignore_code": product[18],
                "descricao": product[3],
                "preco": product[4],
                "custo": product[5],
                "estoque" :product[6],
                "gtin": product[10],
                "peso_liquido": product[11],
                "peso_bruto": product[12],
                "marca": product[13],
                "largura": product[14],
                "altura": product[15],
                "profundidade": product[16],

            }
            #Implementação ignore codes.
            if document["ignore_code"] == 1:
                continue
            if document["internal_error_count"] > 0:
                continue

            all_database_products.append(document)
        self.displayer(f"(database_get_all) {len(all_database_products)} produtos obtidos com sucesso.")
        return all_database_products

    def carbrasil_database_get(self, products)-> list[dict]:

        carbrasil_responses = []
        self.displayer("(carbrasil_database_get) Solicitando produtos ao Banco de Dados CarBrasil")
        for product in products:
            #dimensao1 = altura; dimensao2 = largura; dimensao3 = profundidade;
            response = self._cb_cursor.execute(f"SELECT codprod, descricao, eatu, pvenda, custo_base, peso, dimensao1, dimensao2, dimensao3, gtin_un FROM v_produtos1 WHERE ativa=0 AND codprod={product['codigo_carbrasil']}")
            for x in response:
                document = {
                    "codigo_carbrasil": x[0],
                    "descricao": x[1].strip(),
                    "preco": float(x[3]),
                    "custo": float(x[4]),
                    "estoque": float(x[2]),
                    "peso": float(x[5]),
                    "altura": int(x[6]),
                    "largura": int(x[7]),
                    "profundidade": int(x[8]),
                    "gtin": x[9] if x[9] != "SEM GTIN" else ""
                }
                carbrasil_responses.append(document)

        self.displayer(f"(carbrasil_database_get) {len(carbrasil_responses)} produtos obtidos com sucesso")
        return carbrasil_responses

    def compare_responses(self, carbrasil_response: list[dict], database_response: list[dict]) -> list[dict]:
        
        self.displayer("(compare_responses) Comparando respostas dos dois Bancos de Dados e montando diagnóstico")
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
                    del document["gtin"]
                    del document["altura"]
                    del document["largura"]
                    del document["profundidade"]
                    document["divergencias"] = {}

                    #Comparação
                    equal_description = dict_mysql["descricao"] == dict_carbrasil["descricao"]
                    equal_price = dict_mysql["preco"] == dict_carbrasil["preco"]
                    equal_cost = dict_mysql["custo"] == dict_carbrasil["custo"]
                    equal_stock = dict_mysql["estoque"] == dict_carbrasil["estoque"]
                    equal_gtin = dict_mysql["gtin"] == dict_carbrasil["gtin"]
                    equal_height = dict_mysql["altura"] == dict_carbrasil["altura"]
                    equal_width = dict_mysql["largura"] == dict_carbrasil["largura"]
                    equal_depth = dict_mysql["profundidade"] == dict_carbrasil["profundidade"]

                    if not equal_description:
                        document["divergencias"]["descricao"] = dict_carbrasil["descricao"]
                    if not equal_price:
                        document["divergencias"]["preco"] = dict_carbrasil["preco"]
                    if not equal_cost:
                        document["divergencias"]["custo"] = dict_carbrasil["custo"]
                    if not equal_stock:
                        document["divergencias"]["estoque"] = dict_carbrasil["estoque"]
                    if not equal_gtin:
                        document["divergencias"]["gtin"] = dict_carbrasil["gtin"]
                    if not equal_height:
                        document["divergencias"]["altura"] = dict_carbrasil["altura"]
                    if not equal_width:
                        document["divergencias"]["largura"] = dict_carbrasil["largura"]
                    if not equal_depth:
                         document["divergencias"]["profundidade"] = dict_carbrasil["profundidade"]

                    doc_keys = [x for x in document["divergencias"].keys()]
                    doc_keys_set = set(doc_keys)
                    #Se todas as condições acima foram falsas, doc_keys só terá as chaves padrão, sinalizando que
                    #não há divergência nó código em questão
                    if not doc_keys:
                        break

                    #Se tem ("estoque" ou "custo") E (não tem "preço" e "descrição") nas chaves do dicionário, faz o append e sai do loop
                    #Será redirecionado para API estoque
                    if not {"descricao", "preco", "gtin", "altura", "largura", "profundidade"}.intersection(doc_keys_set):
                        document["divergencias"].setdefault("estoque", dict_carbrasil["estoque"])
                        document["divergencias"].setdefault("custo", dict_carbrasil["custo"]) 
                        document["divergencias"].setdefault("preco", dict_carbrasil["preco"])
                        document["endpoint_correto"] = "estoque"
                        diagnosis.append(document)
                        break

                    #Se tem ("descrição" ou "preco") E (não tem "estoque" e "custo") nas chaves do dicionário, faz o append e sai do loop
                    #Será redirecionado para API produto 
                    if not {"custo", "estoque"}.intersection(doc_keys_set):
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
                    document["divergencias"].setdefault("altura", dict_carbrasil["altura"])
                    document["divergencias"].setdefault("largura", dict_carbrasil["largura"])
                    document["divergencias"].setdefault("profundidade", dict_carbrasil["profundidade"])
                    document["divergencias"].setdefault("gtin", dict_carbrasil["gtin"])
                    document["endpoint_correto"] = "ambos"
                    diagnosis.append(document)
                    break
        self.displayer(f"(compare_responses) Foram encontrados divergências em {len(diagnosis)} produtos.")
        return diagnosis

    def update_mysql_db(self, api_return):
        self.displayer(f"(update_mysql_db) Atualizando {len(api_return)} registros do banco de dados.")
        logger.info(f"(update_mysql_db) Atualizando {len(api_return)} registros do banco de dados.")
        for product in api_return:
            try:
                prod_keys = [x for x in product["divergencias"].keys()]
                if "custo" in prod_keys:
                    self.cursor.execute("UPDATE db_sistema_intermediador SET custo = %s WHERE codigo_carbrasil = %s", (product['divergencias']['custo'], product['codigo_carbrasil']))
                if "estoque" in prod_keys:
                    self.cursor.execute("UPDATE db_sistema_intermediador SET estoque = %s WHERE codigo_carbrasil = %s", (product['divergencias']['estoque'], product['codigo_carbrasil']))
                if "preco" in prod_keys:
                    self.cursor.execute("UPDATE db_sistema_intermediador SET preco = %s WHERE codigo_carbrasil = %s", (product['divergencias']['preco'], product['codigo_carbrasil']))
                if "descricao" in prod_keys:
                    self.cursor.execute("UPDATE db_sistema_intermediador SET descricao = %s WHERE codigo_carbrasil = %s", (product['divergencias']['descricao'], product['codigo_carbrasil']))
            except Exception as err:
                logger.critical(f"Produto no qual o erro foi lançado:\n {product}")
                logger.exception(err)
                raise err

        self.conn.commit()
        self.displayer("(update_mysql_db) Atualização concluída")
        logger.info("(update_mysql_db) Atualização concluída")

    def reset_internal_error_count(self):
        start_time = datetime.datetime.now()
        run_count = 0
        while not self._stop_trigger.is_set():
            self._pause_trigger.wait()
            now = datetime.datetime.now()
            elapsed_time = now - start_time
            elapsed_minutes = elapsed_time.seconds / 60
            if elapsed_minutes >= 60 or run_count == 0:
                run_count+=1
                self._internal_error_cursor.execute("UPDATE db_sistema_intermediador SET internal_error_count = 0 WHERE internal_error_count > 0")
                self._internal_error_conn.commit()

            time.sleep(15)

    def main(self):
        #Checking for exits or pauses
        if not self._pause_trigger.is_set():
            self.UI.modulo3_label.configure(text_color="yellow", text="Modulo 3 (Pausado)")
            self._pause_trigger.wait()
        if self._stop_trigger.is_set():
            return
        
        db_products = self.database_get_all()
        
        if not self._pause_trigger.is_set():
            self.UI.modulo3_label.configure(text_color="yellow", text="Modulo 3 (Pausado)")
            self._pause_trigger.wait()
        if self._stop_trigger.is_set():
            return
        
        carbrasil_products = self.carbrasil_database_get(db_products)
        
        if not self._pause_trigger.is_set():
            self.UI.modulo3_label.configure(text_color="yellow", text="Modulo 3 (Pausado)")
            self._pause_trigger.wait()
        if self._stop_trigger.is_set():
            return
        
        diagnosis = self.compare_responses(carbrasil_response=carbrasil_products, database_response=db_products)
        return diagnosis
    
