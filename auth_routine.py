import base64
import random
import requests
import time
import webbrowser
import string
import os
import json
import datetime
import logging

logging.basicConfig(level=logging.DEBUG, filemode="w", filename="app_logs.log", format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger("auth_routine")

class AuthRoutine():

    app_secrets = open("app_secrets.json")
    parsed = json.load(app_secrets)
    CLIENT_ID = parsed["clientID"]
    CLIENT_SECRET = parsed["clientSecret"]
    CLIENT_ENCODING = f"{CLIENT_ID}:{CLIENT_SECRET}"
    HOST = "https://www.bling.com.br/Api/v3/"
    state = "".join(random.choices(string.ascii_uppercase + string.digits, k=15))

    #ENCODING
    CLIENT_ENCODING_BYTES = CLIENT_ENCODING.encode()
    B64 = base64.b64encode(CLIENT_ENCODING_BYTES)
    B64_STR = B64.decode()

    #Class Vars
    session_tokens = ()
    auth_code = ""
    token_time = None

    def __init__(self, pause_event=None, stop_event=None):
        super().__init__()
        self._pause_trigger = pause_event
        self._stop_trigger = stop_event

    @classmethod
    def first_auth(cls, displayer):
        displayer("(first_auth) Iniciando primeira autenticação")
        logger.info("(first_auth) Iniciando primeira autenticação")
        payload = {
            "client_id": cls.CLIENT_ID,
            "state": cls.state,
            "response_type": "code"
        }
        cn_str = f"{cls.HOST}oauth/authorize"
        r = requests.get(cn_str, params=payload)
        webbrowser.open(r.url)
        time.sleep(2)
        r2 = requests.get("https://cauamarques.pythonanywhere.com/")

        os.system("cls")
        displayer("(first_auth) Aguardando autorização do usuário.")
        while len(r2.text) == 0:
            time.sleep(8)
            r2 = requests.get("https://cauamarques.pythonanywhere.com/")
        else:
            os.system("cls")
            displayer("(first_auth) Código de acesso obtido")
            logger.info("(first_auth) Código de acesso obtido")
            requests.get("https://cauamarques.pythonanywhere.com/clear")
        
        json_auth_code = json.loads(r2.text)
        displayer("(first_auth) Autenticação finalizada")
        logger.info("(first_auth) Autenticação finalizada")
        cls.auth_code = json_auth_code["code"]

    @classmethod
    def second_auth(cls, displayer):
        displayer("(second_auth) Iniciando segunda autenticação")
        logger.info("(second_auth) Iniciando segunda autenticação")

        cn_str = f"{cls.HOST}oauth/token"
        headers = {
        "Content-Type": "application/x-www-form-urlencoded",
        "Accept": "1.0",
        "Authorization": f"Basic {cls.B64_STR}"
        }

        payload = {
        "grant_type":"authorization_code",
        "code":f"{cls.auth_code}"
        }

        r = requests.post(cn_str, headers=headers, data=payload)
        parsed = json.loads(r.text)
        if "error" in parsed.keys():
            logger.critical(f"(second_auth) A api retornou com o seguinte erro: \n{parsed}")
            raise RuntimeError("(second_auth) API RETORNOU COM ERRO")
        
        displayer("(second_auth) Autenticação finalizada.")
        logger.info("(second_auth) Autenticação finalizada.")
        cls.session_tokens = (parsed["access_token"], parsed["refresh_token"])
        cls.token_time = datetime.datetime.now()

    @classmethod
    def refresh(cls):
        logger.info("(refresh) Obtendo novo token de acesso.")
        cn_str = f"{cls.HOST}oauth/token"
        headers = {
        "Content-Type": "application/x-www-form-urlencoded",
        "Accept": "1.0",
        "Authorization": f"Basic {cls.B64_STR}"
        }

        payload = {
        "grant_type":"refresh_token",
        "refresh_token":f"{cls.session_tokens[1]}"
        }

        r = requests.post(cn_str, headers=headers,data=payload)
        parsed = json.loads(r.text)
        cls.session_tokens = (parsed["access_token"], parsed["refresh_token"])
        cls.token_time = datetime.datetime.now()
        logger.info("(refresh) Tokens obtidos com sucesso.")

    @property
    def flags(self):
        return (self._pause_trigger, self._stop_trigger)
    
    @flags.setter
    def flags(self, events):
        self._pause_trigger, self._stop_trigger = events

    def auth_rout(self):
        logger.info("(auth_rout) Rotina de autenticação iniciada")
        while not self._stop_trigger.is_set():
            # #Pause when needed
            self._pause_trigger.wait()

            now = datetime.datetime.now()
            elapsed_time = now - AuthRoutine.token_time
            elapsed_hours = elapsed_time.seconds / 60 / 60 
            if elapsed_hours >= 5.5:
                self.refresh()

            time.sleep(15)