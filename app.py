import threading
import time
import logging
import datetime

#APP modules
import auth_routine
import sync_bling_mysql
import sync_carbrasil_mysql
import request_preprocessing
import data_exchanger

#Init Logging
logging.getLogger("urllib3").setLevel(logging.WARNING)
logging.basicConfig(level=logging.DEBUG, filemode="w", filename="app_logs.log", format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger("app_module")

class ModuleManager():
    """Classe responsável por fazer o controle e a inicialização das Threads, a ponte entre os módulos e também fazer o controle da UI."""

    def __init__(self):
        super().__init__()
        self.pause_event = data_exchanger.PAUSE_EVENT
        self.stop_event = data_exchanger.STOP_EVENT
        self.semaphore = data_exchanger.SEMAPHORE
        
        #UI Instance
        self.UI = data_exchanger.UI
        
        #Configuring Buttons
        self.UI.start_btn.configure(command=self.start)
        self.UI.pause_btn.configure(command=self.pausar_thread)
        self.UI.stop_btn.configure(command=self.parar_thread)
        self.UI.continue_btn.configure(command=self.continuar_thread)

        #Database Module Instance
        self.db_events = sync_carbrasil_mysql.DatabaseSync()
        data_exchanger.DB_EVENTS = self.db_events
        
        #Request Routine Instance
        self.iohandler = request_preprocessing.IOHandler()

        #Authorization Routine Instance
        self.auth_obj = auth_routine.AuthRoutine()

        #Bling Routine Instance
        self.b_routine = sync_bling_mysql.BlingDatabaseSync()
        
        #App Mainloop
        self.UI.root.mainloop()

    def displayer(self, msg: str) -> None:
        """Método responsável por imprimir o texto no console, e inseri-lo em uma textbox na UI"""

        print(msg)
        time = datetime.datetime.now().strftime("%d/%m/%Y %H:%M:%S")
        self.UI.painel_principal_textbox.insert('end', f"{time} - {msg}\n")
        
    def start(self) -> None:
        """
        #Inicializa as threads a partir de duas sub-funções: first_start e second_start.
        
        ##first_start
        Faz a autenticação inicial utilizando a instância auth_obj, proveniente do módulo auth_routine. Chama os métodos
        first_auth e second_auth, e passa o displayer como argumento para esses métodos para que os mesmos imprimam na tela
        e insiram na UI suas mensagens. 
        Após o término dos métodos, ele irá 'setar' o pause_event para True. E permitirá que a função second_start prossiga.

        ##second_start
        Inicializa os métodos: auth_rout, da instância auth_obj; b_routine, da instância bling_routine;
        reset_internal_error_count, da instância db_events; Bridge, da instância desta classe. 
        """
        self.displayer("(start) Inicializando as threads")
        logger.info("(start) Inicializando as threads")
        def first_start():
            self.auth_obj.first_auth(self.displayer)
            self.auth_obj.second_auth(self.displayer)
            self.pause_event.set()
            self.UI.stop_btn.configure(state="normal")
            self.UI.continue_btn.configure(state="normal")
            self.UI.pause_btn.configure(state="normal")

        def second_start():
            #Wait first thread to complete
            self.pause_event.wait()

            #Token refreshing Routine
            self.T1 = threading.Thread(target=self.auth_obj.auth_rout)
            self.T1.start()
            self.displayer("(start) Iniciando módulo de autenticação")
            logger.info("(start) Iniciando módulo de autenticação")

            #Bling & Database Synchronization Routine
            self.T2 = threading.Thread(target=self.b_routine.bling_routine)
            self.T2.start()
            self.displayer("(start) Iniciando módulo 3")
            logger.info("(start) Iniciando módulo 3")
            self.UI.modulo4_label.configure(text_color="green", text="Modulo 3")

            #Resetting Internal Error Count Column
            self.T3 = threading.Thread(target=self.db_events.reset_internal_error_count)
            self.T3.start()
            self.displayer("(start) Iniciando módulo 2")
            logger.info("(start) Iniciando módulo 2")

            #Initizaling request_and_mysql_loop
            self.T4 = threading.Thread(target=self.Bridge)
            self.T4.start()
            self.displayer("(start) Iniciando módulo 1")
            logger.info("(start) Iniciando módulo 1")

            self.T5 = threading.Thread(target=self.b_routine.get_product)
            self.T5.start()
            self.displayer("(start) Iniciando get_product")
            logger.info("(start) Iniciando get_product")

            self.T6 = threading.Thread(target=self.update_ui)
            self.T6.start()
            self.displayer("(start) Iniciando update_ui")
            logger.info("(start) Iniciando update_ui")

        t1 = threading.Thread(target=first_start)
        t1.start()

        t2 = threading.Thread(target=second_start)
        t2.start()

        #Display Effects
        self.UI.start_btn.configure(state="disabled")
        self.UI.painel_principal_label.configure(text_color="green")
        self.UI.modulo2_label.configure(text_color="yellow", text="Modulo 1 (Inicializando...)")
        self.UI.modulo3_label.configure(text_color="yellow", text="Modulo 2 (Inicializando...)")
        self.UI.modulo4_label.configure(text_color="yellow", text="Modulo 3 (Inicializando...)")
    
    def kill_thread(self):
        """Função para assegurar que as Threads serão finalizadas corretamente"""
        while True:
            time.sleep(2)
            if not self.T2.is_alive() and not self.T4.is_alive():
                self.UI.modulo2_label.configure(text="Modulo 1", text_color="red")
                self.UI.modulo3_label.configure(text="Modulo 2", text_color="red")
                self.UI.modulo4_label.configure(text="Modulo 3", text_color="red")
                break

        #Unset events
        self.pause_event.clear()
        self.stop_event.clear()

    def pausar_thread(self):
        """Método responsável por enviar o sinal as Threads pausarem, e modifica cores da UI"""
        #Call the pause method
        self.displayer("(pausar_thread) Pausando a execução do Sistema.")
        logger.info("(pausar_thread) Pausando a execução do Sistema")
        self.pause_event.clear()

        #Display Effects
        self.UI.pause_btn.configure(state="disabled")
        self.UI.continue_btn.configure(state="normal")
        self.UI.modulo2_label.configure(text_color="yellow", text="Modulo 2 (Pausando...)")
        self.UI.modulo3_label.configure(text_color="yellow", text="Modulo 3 (Pausando...)")
        self.UI.modulo4_label.configure(text_color="yellow", text="Modulo 4 (Pausando...)")

    def continuar_thread(self):
        """Método responsável por enviar o sinal para as Threads continuarem, e modifica as cores da UI"""
        #Call the resume method
        self.displayer("(continuar_thread) Continuando a execução do Sistema")
        logger.info("(continuar_thread) Continuando a execução do Sistema")
        self.pause_event.set()

        #Display Effects
        self.UI.pause_btn.configure(state="normal")
        self.UI.continue_btn.configure(state="disabled")
        self.UI.modulo2_label.configure(text="Modulo 2", text_color="green")
        self.UI.modulo3_label.configure(text="Modulo 3", text_color="green")
        self.UI.modulo4_label.configure(text="Modulo 4", text_color="green")

    def parar_thread(self):
        """Método responsável por despausar as Threads pausadas e enviar um sinal para que elas parem, e modifica as cores da UI."""
        #Set Stop Event
        self.displayer("(parar_thread) Parando a execução do Sistema.")
        logger.info("(parar_thread) Parando a execução do Sistema.")
        self.stop_event.set()

        #Resume threads
        self.displayer("(parar_thread) Despausando operações pausadas.")
        logger.info("(parar_thread) Despausando operações pausadas.")
        self.pause_event.set()
        
        #Make sure T2 and T4 are dead
        kill_t = threading.Thread(target=self.kill_thread)
        kill_t.start()

        #Display Effects
        self.UI.stop_btn.configure(state="disabled")
        self.UI.pause_btn.configure(state="disabled")
        self.UI.continue_btn.configure(state="disabled")
        self.UI.start_btn.configure(state="normal")
        self.UI.painel_principal_label.configure(text="Painel Principal", text_color="red")
        self.UI.modulo2_label.configure(text="Modulo 2 (Parando...)",text_color="yellow")
        self.UI.modulo3_label.configure(text="Modulo 3 (Parando...)",text_color="yellow")
        self.UI.modulo4_label.configure(text="Modulo 4 (Parando...)",text_color="yellow")

    #Módulo 1
    def Bridge(self):
        """
        Esse método será executado em uma Thread, e ficará responsável por chamar o método
        main, da instância db_events, e chamar os métodos verify_input e main, da instância iohandler.
        Assim como o nome sugere, esse método faz uma ponte entre a instância db_events, do módulo 
        sync_carbrasil_mysql, e da instância iohandler, do módulo request_preprocessing.

        Ele verifica a cada 15 segundos se foi enviado um sinal para parar/pausar essa Thread.

        Também contabiliza quantas iterações foram feitas.
        """
        start_time = datetime.datetime.now()
        self.bridge_iterations = 0
        self.displayer("(bridge) Iniciando o ciclo de sincronização.")
        logger.info("(bridge) Iniciando o ciclo de sincronização.")
        #Enquanto não for dado o sinal stop_event, ele continuará a iterar
        while not self.stop_event.is_set():
            #Verificando se há pausas a serem feitas
            if not self.pause_event.is_set():
                self.UI.modulo2_label.configure(text="Modulo 2 (Pausado)", text_color="yellow")
                self.pause_event.wait()

            now = datetime.datetime.now()
            elapsed_time = now - start_time
            elapsed_minutes = elapsed_time.seconds / 60

            #Se o tempo decorrido for maior ou igual a 30 minutos, executa o trecho de código abaixo. 
            if elapsed_minutes >= 30 or self.bridge_iterations == 0:
                start_time = datetime.datetime.now()
                self.bridge_iterations+=1

                #Inicio do ciclo de requests
                begin = time.time()
                
                if not self.pause_event.is_set():
                    self.UI.modulo2_label.configure(text="Modulo 2 (Pausado)", text_color="yellow")
                    self.pause_event.wait()
                if self.stop_event.is_set():
                    break

                #Funções principais do ciclo de requests
                self.UI.modulo3_label.configure(text_color="green", text="Modulo 2")
                diagnosis = self.db_events.main()

                if not self.pause_event.is_set():
                    self.UI.modulo2_label.configure(text="Modulo 2 (Pausado)", text_color="yellow")
                    self.pause_event.wait()
                if self.stop_event.is_set():
                    break

                self.UI.modulo2_label.configure(text_color="green", text="Modulo 1")
                self.iohandler.verify_input(diagnosis)

                if not self.pause_event.is_set():
                    self.UI.modulo2_label.configure(text="Modulo 2 (Pausado)", text_color="yellow")
                    self.pause_event.wait()
                if self.stop_event.is_set():
                    break

                self.iohandler.main()
                end = time.time()
                self.displayer(f"(bridge) O programa levou {end-begin:.2f} segundos para completar")
                self.displayer("(bridge) 30 minutos para a próxima sincronização.")
                logger.info(f"(bridge) O programa levou {end-begin:.2f} segundos para completar")

            time.sleep(15)
    
    def update_ui(self):
        while not self.stop_event.is_set():
            if not self.pause_event.is_set():
                self.pause_event.wait()

            #Atualizando informações da label de quantidade de produtos atualizados
            self.UI.info1_count.configure(text=f"{data_exchanger.produtos_atualizados}")

            #Atualizando informação da label de quantidade de iterações do módulo 1
            self.UI.info2_count.configure(text=f"{self.bridge_iterations}")
            time.sleep(15)

app = ModuleManager()
