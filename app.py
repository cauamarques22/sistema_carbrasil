import threading
import logging
import time

#APP modules
import auth_routine
import sync_bling_mysql
import request_routine
import sync_carbrasil_mysql

#Init Logging
logging.getLogger("urllib3").setLevel(logging.WARNING)
logging.basicConfig(level=logging.DEBUG, filemode="w", filename="app_logs.log", format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger("request_routine_module")

def app_loop():
    #First and Second Authentication
    auth_code = auth_routine.first_auth()
    auth_routine.second_auth(auth_code=auth_code)
    
    #Authentication Routine
    T1 = threading.Thread(target=auth_routine.auth_rout)
    T1.start()

    #Bling & Database Synchronization Routine
    b_routine = sync_bling_mysql.BlingDatabaseSync()
    T2 = threading.Thread(target=b_routine.bling_routine)
    T2.start()

    #Instances
    db_events = sync_carbrasil_mysql.DatabaseSync()
    iohandler = request_routine.IOHandler()

    #Resetting Internal Error Count Column
    T3 = threading.Thread(target=db_events.reset_internal_error_count)
    T3.start()

    while True:
        start = time.time()
        diagnosis = db_events.main()
        iohandler.verify_input(diagnosis)
        iohandler.main()
        end = time.time()
        print("(app_loop) 30 minutos para a próxima sincronização.")
        print(f"(app_loop) O programa levou {end-start:.2f} segundos para completar")
        logger.info(f"(app_loop) O programa levou {end-start:.2f} segundos para completar")
        time.sleep(30*60)
    

app_loop()
