import threading
import time
import logging
import datetime

#APP modules
import auth_routine
import sync_bling_mysql
import sync_carbrasil_mysql
import UI
import request_preprocessing
import connect_database

#Init Logging
logging.getLogger("urllib3").setLevel(logging.WARNING)
logging.basicConfig(level=logging.DEBUG, filemode="w", filename="app_logs.log", format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger("app_module")

class ModuleManager(UI.UIFunctions):

    def __init__(self):
        super().__init__()
        self.pause_event = threading.Event()
        self.stop_event = threading.Event()

        #Database Connection
        self.db_connection = connect_database.DatabaseConnection()

        #Database Module Instance
        db_events_conn = self.db_connection.conn_pool.get_connection()
        db_events_cursor = db_events_conn.cursor()
        cb_cursor = self.db_connection.cb_cursor
        self.db_events = sync_carbrasil_mysql.DatabaseSync(txbox=self.modulo3_textbox,conn=db_events_conn, cursor=db_events_cursor,cb_cursor=cb_cursor, pause_event=self.pause_event,stop_event=self.stop_event)
        
        #Request Routine Instance
        self.iohandler = request_preprocessing.IOHandler(txbox=self.modulo2_textbox, db_sync_instance=self.db_events, pause_event=self.pause_event,stop_event=self.stop_event)

        #Authorization Routine Instance
        self.auth_obj = auth_routine.AuthRoutine(pause_event=self.pause_event, stop_event=self.stop_event, principal_txbox=self.modulo1_textbox)

        #Bling Routine Instance
        b_routine_conn = self.db_connection.conn_pool.get_connection()
        b_routine_cursor = b_routine_conn.cursor()
        self.b_routine = sync_bling_mysql.BlingDatabaseSync(txbox=self.modulo4_textbox, conn=b_routine_conn, cursor=b_routine_cursor, pause_event=self.pause_event, stop_event=self.stop_event)
        
        #App Mainloop
        self.root.mainloop()

    def displayer(self, msg):
        print(msg)
        time = datetime.datetime.now().strftime("%d/%m/%Y %H:%M:%S")
        self.modulo1_textbox.insert('end', f"{time} - {msg}\n")

    def start(self):
        self.displayer("(start) Inicializando as threads")
        logger.info("(start) Inicializando as threads")
        def first_start():
            self.auth_obj.first_auth(self.displayer)
            self.auth_obj.second_auth(self.displayer)
            self.pause_event.set()

        def second_start():
            #Wait first thread to complete
            self.pause_event.wait()
            #Token refreshing Routine
            T1 = threading.Thread(target=self.auth_obj.auth_rout)
            T1.start()
            self.displayer("(start) Iniciando módulo de autenticação")
            logger.info("(start) Iniciando módulo de autenticação")
            #Bling & Database Synchronization Routine
            T2 = threading.Thread(target=self.b_routine.bling_routine)
            T2.start()
            self.displayer("(start) Iniciando módulo 3")
            logger.info("(start) Iniciando módulo 3")
            #Resetting Internal Error Count Column
            T3_connection = self.db_connection.conn_pool.get_connection()
            T3_cursor = T3_connection.cursor()
            T3 = threading.Thread(target=self.db_events.reset_internal_error_count, args=(T3_connection, T3_cursor))
            T3.start()
            self.displayer("(start) Iniciando módulo 2")
            logger.info("(start) Iniciando módulo 2")
            #Initizaling request_and_mysql_loop
            T4 = threading.Thread(target=self.Bridge)
            T4.start()
            self.displayer("(start) Iniciando módulo 1")
            logger.info("(start) Iniciando módulo 1")

        t1 = threading.Thread(target=first_start)
        t1.start()

        t2 = threading.Thread(target=second_start)
        t2.start()

        #Display Effects
        self.start_btn.configure(state="disabled")
        self.stop_btn.configure(state="normal")
        self.continue_btn.configure(state="normal")
        self.pause_btn.configure(state="normal")
        self.modulo1_label.configure(text_color="green")
        self.modulo2_label.configure(text_color="green")
        self.modulo3_label.configure(text_color="green")
        self.modulo4_label.configure(text_color="green")

    def pausar_thread(self):
        #Call the pause method
        self.displayer("(pausar_thread) Pausando a execução do Sistema.")
        logger.info("(pausar_thread) Pausando a execução do Sistema")
        self.pause_event.clear()

        #Display Effects
        self.pause_btn.configure(state="disabled")
        self.continue_btn.configure(state="normal")
        self.modulo2_label.configure(text_color="yellow", text="Modulo 2 (Pausado)")
        self.modulo3_label.configure(text_color="yellow", text="Modulo 3 (Pausado)")
        self.modulo4_label.configure(text_color="yellow", text="Modulo 4 (Pausado)")

    def continuar_thread(self):
        #Call the resume method
        self.displayer("(continuar_thread) Continuando a execução do Sistema")
        logger.info("(continuar_thread) Continuando a execução do Sistema")
        self.pause_event.set()

        #Display Effects
        self.pause_btn.configure(state="normal")
        self.continue_btn.configure(state="disabled")
        self.modulo2_label.configure(text="Modulo 2", text_color="green")
        self.modulo3_label.configure(text="Modulo 3", text_color="green")
        self.modulo4_label.configure(text="Modulo 4", text_color="green")

    def parar_thread(self):
        #Call the resume method
        self.displayer("(parar_thread) Despausando operações pausadas.")
        logger.info("(parar_thread) Despausando operações pausadas.")
        self.pause_event.set()
        #Then call the stop method
        self.displayer("(parar_thread) Parando a execução do Sistema.")
        logger.info("(parar_thread) Parando a execução do Sistema.")
        self.stop_event.set()
        

        #Display Effects
        self.stop_btn.configure(state="disabled")
        self.pause_btn.configure(state="disabled")
        self.continue_btn.configure(state="disabled")
        self.start_btn.configure(state="normal")
        self.modulo1_label.configure(text="Painel Principal", text_color="red")
        self.modulo2_label.configure(text="Modulo 2",text_color="red")
        self.modulo3_label.configure(text="Modulo 3",text_color="red")
        self.modulo4_label.configure(text="Modulo 4",text_color="red")

    def Bridge(self):
        start_time = datetime.datetime.now()
        iteration_counter = 0
        self.displayer("(bridge) Iniciando o ciclo de sincronização.")
        logger.info("(bridge) Iniciando o ciclo de sincronização.")
        #Enquanto não for dado o sinal stop_event, ele continuará a iterar
        while not self.stop_event.is_set():
            #Verificando se há pausas a serem feitas
            self.pause_event.wait()

            now = datetime.datetime.now()
            elapsed_time = now - start_time
            elapsed_minutes = elapsed_time.seconds / 60

            #Se o tempo decorrido for maior ou igual a 30 minutos, executa o trecho de código abaixo. 
            if elapsed_minutes >= 30 or iteration_counter == 0:
                start_time = datetime.datetime.now()
                iteration_counter+=1

                #Inicio do ciclo de requests
                begin = time.time()
                if self.stop_event.is_set():
                    break
                #Funções principais do ciclo de requests
                diagnosis = self.db_events.main()
                self.iohandler.verify_input(diagnosis)
                self.iohandler.main()
            
                end = time.time()
                self.displayer(f"(bridge) O programa levou {end-begin:.2f} segundos para completar")
                self.displayer("(bridge) 30 minutos para a próxima sincronização.")
                logger.info(f"(bridge) O programa levou {end-begin:.2f} segundos para completar")
           
            #Atualizando informações da label de quantidade de produtos atualizados
            self.info1_count.configure(text=f"{self.iohandler.produtos_atualizados}")
            time.sleep(15)

app = ModuleManager()