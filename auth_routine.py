import base64
import random
import requests
import time
import webbrowser
import string
import os
import json
import datetime
import sched
import pyautogui


#VARS
app_secrets = open("app_secrets.json")
parsed = json.load(app_secrets)
CLIENT_ID = parsed["clientID"]
CLIENT_SECRET = parsed["clientSecret"]
CLIENT_ENCODING = f"{CLIENT_ID}:{CLIENT_SECRET}"
HOST = "https://www.bling.com.br/Api/v3/"
state = "".join(random.choices(string.ascii_uppercase + string.digits, k=15))
session_tokens = ()

#ENCODING
CLIENT_ENCODING_BYTES = CLIENT_ENCODING.encode()
B64 = base64.b64encode(CLIENT_ENCODING_BYTES)
B64_STR = B64.decode()

def auto_login():
    res = pyautogui.locateCenterOnScreen("locate_bling.png", confidence=0.9)
    pyautogui.moveTo(res[0], res[1]-50)
    pyautogui.click()
    with pyautogui.hold("ctrl"):
        pyautogui.press("f5")
    time.sleep(1)
    pyautogui.moveTo(res[0], res[1]+5)
    pyautogui.click()
    pyautogui.write("carbrasil")
    pyautogui.click()
    time.sleep(0.5)
    pyautogui.moveTo(res[0], res[1]+53)
    pyautogui.click()
    pyautogui.write("Carbrasil@2024")
    res2 = pyautogui.locateCenterOnScreen("entrar.png", confidence=0.9)
    pyautogui.moveTo(res2)
    pyautogui.click()

def first_auth():
    payload = {
        "client_id": CLIENT_ID,
        "state": state,
        "response_type": "code"
    }
    cn_str = f"{HOST}oauth/authorize"
    r = requests.get(cn_str, params=payload)
    webbrowser.open(r.url)
    time.sleep(2)
    r2 = requests.get("https://cauamarques.pythonanywhere.com/")
    auto_login()

    os.system("cls")
    print("Aguardando a Autorização do Usuário.")
    while len(r2.text) == 0:
        time.sleep(8)
        r2 = requests.get("https://cauamarques.pythonanywhere.com/")
    else:
        os.system("cls")
        print("Código de acesso Obtido.")
        print(r2.text)
        requests.get("https://cauamarques.pythonanywhere.com/clear")
    
    auth_code = json.loads(r2.text)
    print("(first_auth) Autenticação de primeiro estágio realizada.")
    return auth_code["code"]

def second_auth(auth_code):
    global session_tokens
    cn_str = f"{HOST}oauth/token"
    headers = {
    "Content-Type": "application/x-www-form-urlencoded",
    "Accept": "1.0",
    "Authorization": f"Basic {B64_STR}"
    }

    payload = {
    "grant_type":"authorization_code",
    "code":f"{auth_code}"
    }

    r = requests.post(cn_str, headers=headers, data=payload)
    parsed = json.loads(r.text)
    if "error" in parsed.keys():
        raise RuntimeError("API RETORNOU COM ERRO (second_auth)")
    
    print("(second_auth) Autenticação de segundo estágio realizada.")
    session_tokens = (parsed["access_token"], parsed["refresh_token"])
    print(session_tokens)

def refresh(refresh_token):
    global session_tokens
    cn_str = f"{HOST}oauth/token"
    headers = {
    "Content-Type": "application/x-www-form-urlencoded",
    "Accept": "1.0",
    "Authorization": f"Basic {B64_STR}"
    }

    payload = {
    "grant_type":"refresh_token",
    "refresh_token":f"{refresh_token}"
    }

    r = requests.post(cn_str, headers=headers,data=payload)
    parsed = json.loads(r.text)
    session_tokens = (parsed["access_token"], parsed["refresh_token"])
    print("(refresh) Tokens Refreshed: ", session_tokens)

def auth_rout():
    while True:
        scheduler = sched.scheduler(time.time, time.sleep)
        t = datetime.datetime.now() + datetime.timedelta(hours=5)
        scheduler.enterabs(t.timestamp() , 1, refresh, argument=(session_tokens[1],))
        print("(auth_routine) Rotina de autenticação agendada.")
        scheduler.run()

