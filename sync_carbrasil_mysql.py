import logging
import time
import datetime

#App Modules
import connect_database
import data_exchanger

logging.basicConfig(filename="app_logs.log", encoding="utf-8", level=logging.DEBUG , format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger("sync_carbrasil_mysql")

class DatabaseSync():
    def __init__(self):
        super().__init__()
        self._pause_trigger = data_exchanger.PAUSE_EVENT
        self._stop_trigger = data_exchanger.STOP_EVENT
        self.UI = data_exchanger.UI

    def displayer(self, msg):
        print(msg)
        time = datetime.datetime.now().strftime("%d/%m/%Y %H:%M:%S")
        self.UI.modulo3_textbox.insert('end', f"{time} - {msg}\n")
        logger.info(msg)

    def database_get_all(self) -> list[dict]:
        """Solicita todos os produtos da tabela do banco de dados e monta um document contendo todas as informações do produto, e faz o append em uma lista que contém
        todos os produtos obtidos do banco de dados."""

        self.displayer("(database_get_all) Solicitando produtos ao Banco de Dados Interno")
        #Selecionando produtos da tabela
        with self.conn.cursor() as cursor:
            cursor.execute("SELECT * FROM db_sistema_intermediador")
            resp = cursor.fetchall()

            #Selecionando colunas da tabela
            cursor.execute("""
            SELECT COLUMN_NAME 
            FROM INFORMATION_SCHEMA.COLUMNS 
            WHERE TABLE_SCHEMA = 'intermediador_bling' 
            AND TABLE_NAME = 'db_sistema_intermediador';
            """)
            columns = cursor.fetchall()
             #columns = [(x, ), (y, ), ...]
    
        all_database_products = []
        for product in resp:
            document = {}
            for column_name in columns:
                document[column_name[0]] = product[columns.index(column_name)]
            #Implementação ignore codes.
            if document["ignore_code"] == 1:
                continue
            if document["internal_error_count"] > 2:
                continue

            all_database_products.append(document)
        self.displayer(f"(database_get_all) {len(all_database_products)} produtos obtidos com sucesso.")
        return all_database_products

    def carbrasil_database_get(self, products, column_list)-> list[dict]:
        
        self.displayer("(carbrasil_database_get) Solicitando produtos ao Banco de Dados CarBrasil")
        carbrasil_responses = []
        column_string = ", ".join([x[0] for x in column_list])
        for product in products:
            #dimensao1 = altura; dimensao2 = largura; dimensao3 = profundidade;
            response = self.cb_cursor.execute(f"SELECT {column_string} FROM v_produtos1 WHERE ativa=0 AND codprod={product['codigo_carbrasil']}")
            for prod in response:
                document = {}
                #Montando o document
                for idx, column in enumerate(column_list):
                    #Regras de data_types
                    if column[0] == "descricao":
                        document[column[1]] = prod[idx].strip()
                    elif column[0] == "gtin_un":
                        document[column[1]] = prod[idx] if prod[idx] != "SEM GTIN" else ""
                    else:
                        document[column[1]] = column[2](prod[idx])
                
                #Append do document
                carbrasil_responses.append(document)

        self.displayer(f"(carbrasil_database_get) {len(carbrasil_responses)} produtos obtidos com sucesso")
        return carbrasil_responses

    def compare_responses(self, carbrasil_response: list[dict], database_response: list[dict], chaves_padrao: list[str], chaves_comparacao: list[str]) -> list[dict]:
        
        self.displayer("(compare_responses) Comparando respostas dos dois Bancos de Dados e montando diagnóstico")
        diagnosis = []
        for dict_mysql in database_response:
            for dict_carbrasil in carbrasil_response:
                if dict_mysql["codigo_carbrasil"] == dict_carbrasil["codigo_carbrasil"]:

                    #Criação do document
                    document = {}
                    default_keys = chaves_padrao
                    for key in default_keys:
                        document[key] = dict_mysql[key]
                    document["divergencias"] = {}

                    #Comparação
                    comparison_keys = chaves_comparacao
                    for comp_key in comparison_keys:
                        if not dict_mysql[comp_key] == dict_carbrasil[comp_key]:
                            document["divergencias"][comp_key] = dict_carbrasil[comp_key]

                    doc_keys = [x for x in document["divergencias"].keys()]
                    doc_keys_set = set(doc_keys)
                    #Se não tem nada na chave "divergencias" sai do loop
                    if not doc_keys:
                        break

                    #Se tem apenas "custo" e/ou "estoque" nas chaves do dicionário será redirecionado para API estoque
                    if not doc_keys_set.difference({"custo", "estoque"}):
                        padrao_api_estoque = ["estoque", "custo", "preco"]
                        for key in padrao_api_estoque:
                            document["divergencias"].setdefault(key, dict_carbrasil[key])
                        document["endpoint_correto"] = "estoque"
                        diagnosis.append(document)
                        break

                    #Se não tiver "custo" ou "estoque" nas chaves do dicionário será redirecionado para API produto 
                    if not {"custo", "estoque"}.intersection(doc_keys_set):
                        padrao_api_produto = ["descricao", "preco", "altura", "largura", "profundidade", "gtin", "peso"] 
                        for key in padrao_api_produto:
                            document["divergencias"].setdefault(key, dict_carbrasil[key])
                        document["endpoint_correto"] = "produto"
                        diagnosis.append(document)
                        break

                    #Caso nenhuma condição sirva o dicionário passará por duas APIs
                    document["divergencias"] = dict_carbrasil
                    del document["divergencias"]["codigo_carbrasil"]
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
                for key in prod_keys:
                    with self.conn.cursor() as cursor:
                        cursor.execute(f"UPDATE db_sistema_intermediador SET {key} = %s WHERE codigo_carbrasil = %s", (product['divergencias'][key], product['codigo_carbrasil']))

            except Exception as err:
                logger.critical(f"Produto no qual o erro foi lançado:\n {product}")
                logger.exception(err)
                raise err

        self.conn.commit()
        self.conn.close()
        self.displayer("(update_mysql_db) Atualização concluída")
        logger.info("(update_mysql_db) Atualização concluída")

    #Está lançando DatabaseError 1205
    #Está lançando OperationalError 2013: Lost Connection to Mysql server during query
    def reset_internal_error_count(self):
        start_time = datetime.datetime.now()
        run_count = 0
        while not self._stop_trigger.is_set():
            self._pause_trigger.wait()
            now = datetime.datetime.now()
            elapsed_time = now - start_time
            elapsed_minutes = elapsed_time.seconds / 60
            if elapsed_minutes >= 60 or run_count == 0:
                start_time = datetime.datetime.now()
                exception_count = 0
                #Database Connection
                self.internal_error_conn = connect_database.conn_pool.get_connection()
                self.displayer(f"Internal Error Connection Id: {self.internal_error_conn.connection_id}")
                while True:
                    try:
                        run_count+=1
                        with self.internal_error_conn.cursor() as ierror_cursor:
                            ierror_cursor.execute("UPDATE db_sistema_intermediador SET internal_error_count = 0 WHERE internal_error_count > 0")
                        self.internal_error_conn.commit()
                        logger.info(f"(reset_internal_error_count) Atualização de internal_error_count feita com sucesso. Exceções: {exception_count}")
                        
                        #Close Database Connection
                        self.internal_error_conn.close()
                        break
                    except Exception as err:
                        logger.warn("(reset_internal_error_count) Uma exceção foi encontrada ao tentar atualizar o internal_error_count, tentando novamente")
                        logger.exception(err)

                        self.UI.error_textbox.insert("end", "(reset_internal_error_count) Uma exceção foi encontrada ao tentar atualizar o internal_error_count, tentando novamente")
                        exception_count+=1
                        if exception_count == 3:
                            logger.critical("(reset_internal_error_count) Quantidade de errors permitidos excedida.")
                            raise err
            time.sleep(15)

    def main(self):
        #Checking for exits or pauses
        if not self._pause_trigger.is_set():
            self.UI.modulo3_label.configure(text_color="yellow", text="Modulo 3 (Pausado)")
            self._pause_trigger.wait()
        if self._stop_trigger.is_set():
            return
                
        #Database Connections
        self.conn = connect_database.conn_pool.get_connection()
        self.cb_conn = connect_database.get_cb_connection()
        self.cb_cursor = self.cb_conn.cursor()
        self.displayer(f"DB Events Connection Id: {self.conn.connection_id}")
        db_products = self.database_get_all()
        
        #Checking for exits or pauses
        if not self._pause_trigger.is_set():
            self.UI.modulo3_label.configure(text_color="yellow", text="Modulo 3 (Pausado)")
            self._pause_trigger.wait()
        if self._stop_trigger.is_set():
            return
        
        #(nome_da_coluna_db, nome_da_chave_do_dicionario, datatype)
        column_list = [("codprod", "codigo_carbrasil", int), ("descricao", "descricao", str), ("eatu", "estoque", int), 
                       ("pvenda", "preco", float), ("custo_base", "custo", float), ("peso", "peso", float), ("dimensao1", "altura", int),
                       ("dimensao2", "largura", int), ("dimensao3", "profundidade", int), ("gtin_un", "gtin", str), ("codncm", "ncm", str)]
        carbrasil_products = self.carbrasil_database_get(db_products, column_list)
        
        #Checking for exits or pauses
        if not self._pause_trigger.is_set():
            self.UI.modulo3_label.configure(text_color="yellow", text="Modulo 3 (Pausado)")
            self._pause_trigger.wait()
        if self._stop_trigger.is_set():
            return
        
        #chave_comparacao: chaves que serão comparadas. (dict_mysql[chave] == dict_carbrasil[chave])
        chave_padrao = ["codigo_carbrasil", "id_bling", "idEstoque", "bling_tipo", "bling_formato", "bling_situacao", "marca", "internal_error_count"]
        chave_comparacao = ["descricao", "preco", "custo", "estoque", "gtin", "altura", "largura", "profundidade", "peso", "ncm"]
        diagnosis = self.compare_responses(carbrasil_response=carbrasil_products, database_response=db_products, chaves_padrao=chave_padrao, chaves_comparacao=chave_comparacao)
        
        
        self.cb_conn.close()
        return diagnosis
    
