#Import libraries
import requests
import json
import time
import logging
import datetime
from threading import Semaphore

#APP modules
import auth_routine
import connect_database

logger = logging.getLogger(__name__)
logging.basicConfig(filename="app_logs.log", encoding="utf-8", level=logging.DEBUG, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

class BlingDatabaseSync():
    def __init__(self, UI, semaphore: Semaphore,pause_event=None, stop_event=None):
        super().__init__()
        self._pause_trigger = pause_event
        self._stop_trigger = stop_event
        self.bling_products = []
        self.txbox = UI.modulo4_textbox
        self.UI = UI
        self.semaphore = semaphore

    def displayer(self, msg):
        print(msg)
        time = datetime.datetime.now().strftime("%d/%m/%Y %H:%M:%S")
        self.txbox.insert('end', f"{time} - {msg}\n")
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

    def update_database(self):
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
                continue
        self.conn.commit()
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
                self.conn = connect_database.conn_pool.get_connection()
                self.semaphore.acquire()
                self.start_time = datetime.datetime.now()
                iteration_count+=1
                
                #Make request and close semaphore
                self.api_calls_get()
                self.semaphore.release()

                #Make database update and close connection
                self.update_database()
                self.conn.close()

            time.sleep(15)


