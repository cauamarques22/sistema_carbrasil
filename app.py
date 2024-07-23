import threading
import time
import logging
import datetime
from queue import Queue

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
        self.db_events = sync_carbrasil_mysql.DatabaseSync(txbox=self.modulo3_textbox,conn=db_events_conn, cursor=db_events_cursor, pause_event=self.pause_event,stop_event=self.stop_event)
        
        #Request Routine Instance
        self.iohandler = request_preprocessing.IOHandler(txbox=self.modulo2_textbox, db_sync_instance=self.db_events, pause_event=self.pause_event,stop_event=self.stop_event)

        #Authorization Routine Instance
        self.auth_obj = auth_routine.AuthRoutine(pause_event=self.pause_event, stop_event=self.stop_event)

        #Bling Routine Instance
        b_routine_conn = self.db_connection.conn_pool.get_connection()
        b_routine_cursor = b_routine_conn.cursor()
        self.b_routine = sync_bling_mysql.BlingDatabaseSync(txbox=self.modulo4_textbox, conn=b_routine_conn, cursor=b_routine_cursor, pause_event=self.pause_event, stop_event=self.stop_event)
        
        #App Mainloop
        self.root.mainloop()

    def start(self):
        
        def first_start():
            self.auth_obj.first_auth()
            self.auth_obj.second_auth()
            self.pause_event.set()

        def second_start():
            #Wait first thread to complete
            self.pause_event.wait()
            #Token refreshing Routine
            T1 = threading.Thread(target=self.auth_obj.auth_rout)
            T1.start()
            #Bling & Database Synchronization Routine
            T2 = threading.Thread(target=self.b_routine.bling_routine)
            T2.start()
            #Resetting Internal Error Count Column
            T3_connection = self.db_connection.conn_pool.get_connection()
            T3_cursor = T3_connection.cursor()
            T3 = threading.Thread(target=self.db_events.reset_internal_error_count, args=(T3_connection, T3_cursor))
            T3.start()
            #Initizaling request_and_mysql_loop
            T4 = threading.Thread(target=self.Bridge)
            T4.start()

        t1 = threading.Thread(target=first_start)
        t1.start()

        t2 = threading.Thread(target=second_start)
        t2.start()

        #Display Effects
        self.start_btn.configure(state="disabled")
        self.stop_btn.configure(state="normal")
        self.continue_btn.configure(state="normal")
        self.pause_btn.configure(state="normal")
        self.modulo2_label.configure(text_color="green")
        self.modulo3_label.configure(text_color="green")
        self.modulo4_label.configure(text_color="green")

    def pause_thread(self):
        #Call the pause method 
        self.pause_event.clear()

        #Display Effects
        self.pause_btn.configure(state="disabled")
        self.continue_btn.configure(state="normal")
        self.modulo2_label.configure(text_color="yellow", text="Modulo 2 (Pausado)")
        self.modulo3_label.configure(text_color="yellow", text="Modulo 3 (Pausado)")
        self.modulo4_label.configure(text_color="yellow", text="Modulo 4 (Pausado)")

    def continuar_thread(self):
        #Call the resume method
        self.pause_event.set()

        #Display Effects
        self.pause_btn.configure(state="normal")
        self.continue_btn.configure(state="disabled")
        self.modulo2_label.configure(text="Modulo 2", text_color="green")
        self.modulo3_label.configure(text="Modulo 3", text_color="green")
        self.modulo4_label.configure(text="Modulo 4", text_color="green")

    def parar_thread(self):
        #Call the resume method
        self.pause_event.set()
        #Then call the stop method
        self.stop_event.set()

        #Display Effects
        self.stop_btn.configure(state="disabled")
        self.pause_btn.configure(state="disabled")
        self.continue_btn.configure(state="disabled")
        self.start_btn.configure(state="normal")
        self.modulo2_label.configure(text="Modulo 2",text_color="red")
        self.modulo3_label.configure(text="Modulo 3",text_color="red")
        self.modulo4_label.configure(text="Modulo 4",text_color="red")

    def Bridge(self):
        start_time = datetime.datetime.now()
        iteration_counter = 0
        while not self.stop_event.is_set():
            self.pause_event.wait()
            now = datetime.datetime.now()
            elapsed_time = now - start_time
            elapsed_minutes = elapsed_time.seconds / 60
            if elapsed_minutes >= 30 or iteration_counter == 0:
                iteration_counter+=1
                begin = time.time()
                diagnosis = self.db_events.main()
                if self.stop_event.is_set():
                    break

                #Mudar o IOHANDLER para ser assincrono de verdade e criar compartilhamento de variáveis com
                #as funções call_api e verify_input, pos eu não posso depender de retorno delas.
                self.iohandler.product_queue = Queue(maxsize=4)
                self.iohandler.yes_stock_queue = Queue(maxsize=4)
                self.iohandler.no_stock_queue = Queue(maxsize=4)

                mthread1 = threading.Thread(target=self.iohandler.run_verify_input, args=(diagnosis,)) 
                mthread2 = threading.Thread(target=self.iohandler.run_call_api)
                mthread1.start()
                mthread2.start()
                mthread1.join()
                mthread2.join()

                end = time.time()
                runtime = end - begin
                #self.displayer("(app_loop) 30 minutos para a próxima sincronização.")
                #self.displayer(f"(app_loop) O programa levou {runtime:.2f} segundos para completar")
                logger.info(f"(app_loop) O programa levou {runtime:.2f} segundos para completar")
            time.sleep(15)

app = ModuleManager()