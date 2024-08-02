import threading
import UI

#Shared Events
PAUSE_EVENT = threading.Event()
STOP_EVENT = threading.Event()

#Shared Semaphores
SEMAPHORE = threading.Semaphore(1)

#Shared Instances
UI = UI.UIFunctions()
DB_EVENTS = None

#Shared Variables
produtos_atualizados = 0

