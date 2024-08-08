import requests
import json
import time
import logging
import datetime

#APP modules
import auth_routine
import connect_database
import data_exchanger

logger = logging.getLogger(__name__)
logging.basicConfig(filename="app_logs.log", encoding="utf-8", level=logging.DEBUG, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

class BlingDatabaseSync():
    def __init__(self):
        self._pause_trigger = data_exchanger.PAUSE_EVENT
        self._stop_trigger = data_exchanger.STOP_EVENT
        self.UI = data_exchanger.UI
        self.semaphore = data_exchanger.SEMAPHORE

        self.bling_products = []

    def displayer(self, msg):
        print(msg)
        time = datetime.datetime.now().strftime("%d/%m/%Y %H:%M:%S")
        self.UI.modulo4_textbox.insert('end', f"{time} - {msg}\n")
        logger.info(msg)

    def api_calls_get(self):
        pages = 1
        products_per_page = []
        all_products = []
        self.displayer("(api_calls_get) Solicitando produtos ao Bling.")
        while not self._stop_trigger.is_set():
            self._pause_trigger.wait()
            time.sleep(1.5)
            headers = {
                "Authorization": f"Bearer {auth_routine.AuthRoutine.session_tokens[0]}"
            }

            payload = {
                "limite": 500,
                "pagina": pages,
                "criterio": 2
            }

            r = requests.get(f"{auth_routine.AuthRoutine.HOST}produtos", params=payload, headers=headers)
            parsed = json.loads(r.text)

            if not parsed.get("data"):
                break

            products_per_page.append(parsed["data"])
            pages +=1
        
        for x in products_per_page:
            for prod in x:
                all_products.append(prod)
        
        self.bling_products = all_products
        self.displayer(f"(api_calls_get) {len(self.bling_products)} Produtos recebidos com sucesso.")

    #Erro API 504
    def get_product(self):
        self.start_time_2 = datetime.datetime.now()
        iteration_count = 0

        while not self._stop_trigger.is_set():
            if not self._pause_trigger.is_set():
                self._pause_trigger.wait()
            
            now = datetime.datetime.now()
            elapsed_time = now - self.start_time_2
            elapsed_minutes = elapsed_time.seconds / 60
            if elapsed_minutes >= 120 or iteration_count == 0 and not self._stop_trigger.is_set():
                iteration_count+=1
                conn = connect_database.conn_pool.get_connection()
                with conn:
                    with conn.cursor() as cursor:
                        cursor.execute("SELECT * FROM db_sistema_intermediador")
                        response = cursor.fetchall()
                
                for x in response:
                    if self._stop_trigger.is_set():
                        break
                    if not self._pause_trigger.is_set():
                        self._pause_trigger.wait()

                    print(f"(get_product) getting {x[0]} info from bling")
                    self.semaphore.acquire()
                    time.sleep(0.5)
                    headers = {
                        "Authorization": f"Bearer {auth_routine.AuthRoutine.session_tokens[0]}"
                    }
                    
                    r = requests.get(f"{auth_routine.AuthRoutine.HOST}produtos/{x[1]}", headers=headers, timeout=(10, 10))
                    self.semaphore.release()
                    #Erro 504 no código 9624 e 9625
                    parsed = json.loads(r.text)
                    parsed = parsed["data"]
                    document = {
                        "codigo_carbrasil": int(parsed["codigo"]),
                        "descricao": parsed["nome"],
                        "preco": parsed["preco"],
                        "gtin": parsed["gtin"],
                        "ncm": parsed["tributacao"]["ncm"],
                        "peso": parsed["pesoLiquido"],
                        "marca": parsed["marca"],
                        "largura": parsed["dimensoes"]["largura"],
                        "altura": parsed["dimensoes"]["altura"],
                        "profundidade": parsed["dimensoes"]["profundidade"]
                    }
                    #Pool exhausted
                    conn = connect_database.conn_pool.get_connection()
                    with conn:
                        with self.conn2.cursor() as cursor:
                            columns = ["descricao", "preco", "gtin", "ncm", "peso", "marca", "largura", "altura", "profundidade"]
                            for column in columns:
                                cursor.execute(f"UPDATE db_sistema_intermediador SET {column} = %s WHERE codigo_carbrasil = %s", (document[column], document["codigo_carbrasil"]))
                    time.sleep(0.3)
            time.sleep(15)
    def update_database(self):
        self.conn = connect_database.conn_pool.get_connection()
        self.displayer(f"(update_database) Iniciando atualização em {len(self.bling_products)} produtos do Banco de Dados.")
        for item in self.bling_products:
            #Verifying triggers
            self._pause_trigger.wait()
            if self._stop_trigger.is_set():
                return
            
            try:
                with self.conn.cursor() as cursor:
                    cursor.execute(f"SELECT * FROM db_sistema_intermediador WHERE codigo_carbrasil = {int(item['codigo'])}")
                    response = cursor.fetchall()
                    if not response:
                        cursor.execute(f"""INSERT INTO db_sistema_intermediador \
                                    (codigo_carbrasil, id_bling, descricao, preco, bling_tipo, bling_formato, bling_situacao) \
                                    VALUES ({int(item['codigo'])}, {item['id']}, '{item['nome']}', {item['preco']}, '{item['tipo']}', '{item['formato']}', '{item['situacao']}')""")
            except ValueError as err:
                logger.error(f"(update_database) Produtos retornaram com erros: \n{item}")
                logger.error(err)
                self.UI.error_textbox.insert("end",f"(update_database) Produtos retornaram com erros:\n{item}")
                continue
        self.conn.commit()
        self.conn.close()
        self.displayer("(update_database) Atualização Concluída.")

    def bling_routine(self):
        self.start_time = datetime.datetime.now()
        iteration_count = 0
        
        while not self._stop_trigger.is_set():
            #Pause when needed
            if not self._pause_trigger.is_set():
                self.UI.modulo4_label.configure(text="Modulo 4 (Pausado)", text_color="yellow")
                self._pause_trigger.wait()
            
            now = datetime.datetime.now()
            elapsed_time = now - self.start_time
            elapsed_minutes = elapsed_time.seconds / 60  
            if elapsed_minutes >= 25 or iteration_count == 0 and not self._stop_trigger.is_set():
                
                self.semaphore.acquire()
                self.start_time = datetime.datetime.now()
                iteration_count+=1
                
                #Make request and release semaphore
                self.api_calls_get()
                self.semaphore.release()

                #Make database update 
                self.update_database()
                

            time.sleep(15)


