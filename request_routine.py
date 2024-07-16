#Dependencies
import aiohttp.client_exceptions
import auth_routine
import datetime
import aiohttp
import asyncio
import itertools
import logging


#APP modules
from sync_bling_mysql import cursor, conn

logging.basicConfig(level=logging.DEBUG, filemode="a", filename="app_logs.log", format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger("request_routine_module")

class ApiFunctions():
    def __init__ (self):
        self.semaphore = asyncio.Semaphore(1)

    async def error_handler(self, semaphore,function, **kwargs):
        async with semaphore:
            for key,value in kwargs.items():
                if function == "update_stock_main":
                    if key == "error_not_found" and kwargs[key]:
                        msg = "(error_handler) PRODUTOS DA FUNÇÃO 'update_stock_main' RETORNARAM COM 'error_not_found'"
                        print(msg)
                        logger.info(msg)

                        r = await asyncio.gather(self.create_stock_main(value, error=True))
                        if r:
                            print("(error_handler) ERROS CORRIGIDOS")
                            logger.info("(error_handler) ERROS CORRIGIDOS")
                        else:
                            print("(error_handler) ERROS NÃO FORAM CORRIGIDOS")
                            logger.error("(error_handler) ERROS NÃO FORAM CORRIGIDOS")
                    elif key == "unknown_errors" and kwargs[key]:
                        msg = "(error_handler) PRODUTOS DA FUNÇÃO 'update_stock_main' RETORNARAM COM 'unknown_errors'"
                        print(msg)
                        logger.error(msg)
                        logger.error(value)

                    elif key == "os_error" and kwargs[key]:
                        msg = "(error_handler) PRODUTOS DA FUNÇÃO 'update_stock_main' RETORNARAM COM 'os_error'"
                        print(msg)
                        logger.error(msg)
                        logger.error(value)

                elif function == "create_stock_main":
                    if key == "unknown_errors" and kwargs[key]:
                        msg = "(error_handler) PRODUTOS DA FUNÇÃO 'create_stock_main' RETORNARAM COM 'unknown_errors'"
                        print(msg)
                        logger.error(msg)
                        logger.error(value)

                    elif key == "os_error" and kwargs[key]:
                        msg = "(error_handler) PRODUTOS DA FUNÇÃO 'create_stock_main' RETORNARAM COM 'os_error'"
                        print(msg)
                        logger.error(msg)
                        logger.error(value)
                    
                    elif key == "error_not_found" and kwargs[key]:
                        msg = "(error_handler) PRODUTOS DA FUNÇÃO 'create_stock_main' RETORNARAM COM 'error_not_found'"
                        print(msg)
                        logger.error(msg)
                        logger.error(value)

                elif function == "update_product_main":
                    if key == "unknown_errors" and kwargs[key]:
                        msg = "(error_handler) PRODUTOS DA FUNÇÃO 'update_product_main' RETORNARAM COM 'unknown_errors'"
                        print(msg)
                        logger.error(msg)
                        logger.error(value)

                    elif key == "os_error" and kwargs[key]:
                        msg = "(error_handler) PRODUTOS DA FUNÇÃO 'update_product_main' RETORNARAM COM 'os_error'"
                        print(msg)
                        logger.error(msg)
                        logger.error(value)
                    
                    elif key == "error_not_found" and kwargs[key]:
                        msg = "(error_handler) PRODUTOS DA FUNÇÃO 'update_product_main' RETORNARAM COM 'error_not_found'"
                        print(msg)
                        logger.error(msg)
                        logger.error(value)

    async def create_stock_slave(self, session, product):

        print("(create_stock_slave) Começando request: ", product["codigo_carbrasil"])
        logger.debug(f"(create_stock_slave) Começando request: \n{product['codigo_carbrasil']}")
        headers = {
            "Authorization": f"Bearer {auth_routine.session_tokens[0]}"
        }
        payload = {
            "produto":{
                "id": product["id_bling"]
            },
            "deposito":{
                "id": 3471220462
            },
            "operacao": "B",
            "preco": product["divergencias"]["preco"],
            "custo": product["divergencias"]["custo"],
            "quantidade": product["divergencias"]["estoque"],
            "observacoes": "API CARBRASIL-BLING POST",
            "data": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }

        try:
            async with session.request(method="POST",url=f"{auth_routine.HOST}estoques", headers=headers, json=payload) as resp:
                response = await resp.json()
                if resp.status == 201:
                    document = {"response_status": resp.status,"codigo_carbrasil":product["codigo_carbrasil"], "id_estoque": response["data"]["id"]}
                    logger.debug(f"request document: \n{document}")
                    return document
                
                product["response_status"] = resp.status
                product["decoded_resp"] = response
                logger.error(f"Produto com erro: \n{product}")
                return product
        
        except aiohttp.client_exceptions.ClientOSError as err:

            logger.error(f"(create_stock_slave) WINERROR: {err.winerror}")
            logger.error("(create_stock_slave) PRODUCT THAT RAISED AN ERROR:")
            logger.error(f"(create_stock_slave) {product}")
            logger.exception("ClientOSError")
            product["response_status"] = "OSERROR"
            return product
        
        except Exception as err:
            
            product["response_status"] = "CRITICAL_EXCEPTION"
            logger.critical("(create_stock_slave) UNHANDLED EXCEPTION")
            logger.exception("(create_stock_slave) UNKNOWN EXCEPTION")
            logger.debug(f"Produto utilizado durante a exceção:\n{product}")
            raise err

    async def create_stock_main(self, instructions, error=False):
        
        print(f"{'(create_stock_main: errors)' if error else '(create_stock_main)'} Começando POST REQUESTS")
        unknown_errors: list[dict] = []
        os_error: list[dict] = []
        critical_errors: list[dict] = []
        ok_status: list[dict] = []

        batched_products = list(itertools.batched(instructions, 3))
        async with aiohttp.ClientSession() as session:
            for product in batched_products:
                await asyncio.sleep(1.1)
                tasks = [asyncio.create_task(self.create_stock_slave(session, p)) for p in product]
                response = await asyncio.gather(*tasks)
                for resp in response:

                    #ERROR HANDLING
                    if "decoded_resp" in resp.keys():
                        if "error" in resp["decoded_resp"].keys():
                            unknown_errors.append(resp)
                    
                    if resp["response_status"] == "OSERROR":
                        os_error.append(resp)
                    elif resp["response_status"] == "CRITICAL_EXCEPTION":
                        critical_errors.append(resp)
                    elif resp["response_status"] == 201:
                        ok_status.append(resp)
        
        for prod in ok_status:
            cursor.execute(f"UPDATE db_sistema_intermediador SET idEstoque={prod['id_estoque']} WHERE codigo_carbrasil={prod['codigo_carbrasil']}")
        conn.commit()
        if unknown_errors or os_error or critical_errors:
            await asyncio.create_task(self.error_handler(semaphore=self.semaphore, function="create_stock_main", unknown_errors=unknown_errors, 
                            os_error=os_error, critical_errors=critical_errors))
        logger.info("(create_stock_main) POST REQUESTS finalizadas com sucesso.")
        print("(create_stock_main) POST REQUESTS finalizadas com sucesso.")

    async def update_stock_slave(self, session:aiohttp.ClientSession, product: dict) -> dict:

        print("(update_stock_slave) Começando request: ", product["codigo_carbrasil"])
        logger.info(f"(update_stock_slave) Começando request: {product["codigo_carbrasil"]}")
        headers = {
        "Authorization": f"Bearer {auth_routine.session_tokens[0]}"
        }
        payload = {
            "operacao": "B",
            "preco": product["divergencias"]["preco"],
            "custo": product["divergencias"]["custo"],
            "quantidade": product["divergencias"]["estoque"],
            "observacoes": "API CARBRASIL-BLING PUT",
            "data": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }
        try:
            async with session.request(method="PUT", url=f"{auth_routine.HOST}estoques/{product['id_estoque']}",  json=payload, headers=headers) as resp:
                resp_status = resp.status

                if resp_status  != 204:
                    product["decoded_resp"] = resp.json()
                    product["response_status"]= resp_status 
                    logger.error(f"Produto com erro: \n{product}")
                    return product

                product["response_status"] = resp_status 
                logger.debug(product)
                return product

        except aiohttp.client_exceptions.ClientOSError as err:
            
            product["response_status"] = "OSERROR"
            logger.error(f"(api_estoque_put) WINERROR: {err.winerror}")
            logger.error("(api_estoque_put) PRODUCT THAT RAISED AN ERROR:")
            logger.error(f"(api_estoque_put): \n{product}")
            logger.exception("ClientOSError")
            return product
        
        except Exception as err:
            
            product["response_status"] = "CRITICAL_EXCEPTION"
            logger.critical("(update_stock_slave) UNHANDLED EXCEPTION")
            logger.exception("(update_stock_slave) UNKNOWN EXCEPTION")
            logger.error(f"(update_stock_slave): \n{product}")
            raise err

    async def update_stock_main(self, instructions):
        print(f"(update_stock_main) Começando PUT REQUESTS em {len(instructions)} registros")
        error_not_found: list[dict] = []
        unknown_errors: list[dict] = []
        os_error: list[dict] = []
        ok_status: list[dict] = []

        batched_products = list(itertools.batched(instructions, 3))
        async with aiohttp.ClientSession() as session:
            for product in batched_products:
                await asyncio.sleep(1.1)
                tasks = [asyncio.create_task(self.update_stock_slave(session,p)) for p in product]
                responses = await asyncio.gather(*tasks)
               
                #ERROR HANDLING
                for resp in responses:
                    
                    if "decoded_resp" in resp.keys():
                        if "error" in resp["decoded_resp"].keys():
                            if resp["decoded_resp"]["errror"]["type"] == "RESOURCE_NOT_FOUND":
                                error_not_found.append(resp)
                                continue
                            unknown_errors.append(resp)
                    
                    if resp["response_status"] == "OSERROR":
                        os_error.append(resp)
                    elif resp["response_status"] == 204:
                        ok_status.append(resp)

        logger.info(f"(update_stock_main) PUT REQUESTS FINALIZADAS, RESULTADOS:\n \
                    error_not_found:{len(error_not_found)}\n \
                    unknown_errors:{len(unknown_errors)}\n \
                    os_errors: {len(os_error)}\n")
        
        if error_not_found or unknown_errors or os_error:
            await asyncio.create_task(self.error_handler(semaphore=self.semaphore ,function="update_stock_main" ,
                                                         error_not_found=error_not_found, unknown_errors=unknown_errors,
                                                         os_error=os_error))
        
        print("(update_stock_main) ROTINA DE PUT REQUESTS FINALIZADA")
        logger.info("(update_stock_main) ROTINA DE PUT REQUESTS FINALIZADA")

    async def update_product_slave(self, session: aiohttp.ClientSession, product: dict): #ATUALIZAR O ERROR HANDLING
        
        print(f"(update_product_slave) Atualizando Código: {product["codigo_carbrasil"]}")
        logger.info(f"(update_product_slave) Atualizando Código: {product["codigo_carbrasil"]}")
        headers = {
        "Authorization": f"Bearer {auth_routine.session_tokens[0]}"
        }
        payload = {
            "nome": product["descricao"],
            "tipo": product["bling_tipo"],
            "situacao": product["bling_situacao"],
            "formato": product["bling_formato"],
            "codigo": product["codigo_carbrasil"],
            "unidade": "UN",
            "condicao": 1,
            "tipoProducao": "T",

        }
        if "preco" in product.keys():
            payload["preco"] = product["preco"]
        
        try:
            async with session.request(method="PUT", url=f"{auth_routine.HOST}produtos/{product['id_bling']}", headers=headers, json=payload) as resp:
                resp_status = resp.status
                
                if resp_status != 200:
                    product["decoded_resp"] = resp.json()
                    product["response_status"]= resp_status 
                    logger.error(f"Produto com erro: \n{product}")
                    return product

                product["response_status_code"] = resp_status.status
                logger.debug(f"Código Car Brasil: {product['codigo_carbrasil']} | Status: {product['response_status_code']}")
                return product

        except aiohttp.client_exceptions.ClientOSError as err:
            product["response_status_code"] = resp.status
            logger.error(f"(api_estoque_put) WINERROR: {err.winerror}")
            logger.error("(api_estoque_put) PRODUCT THAT RAISED AN ERROR:")
            logger.error(f"(api_estoque_put) {product}")
            logger.exception("ClientOSError")
            return product
        
        except Exception as err:
            product["response_status"] = "CRITICAL_EXCEPTION"
            logger.critical("(update_stock_slave) UNHANDLED EXCEPTION")
            logger.exception("(update_stock_slave) UNKNOWN EXCEPTION")
            logger.error(f"(update_stock_slave): \n{product}")
            raise err

    async def update_product_main(self,instructions: list[dict]):
        print("(update_product_main) Atualizando informações de PRODUTOS no Bling..")
        error_not_found: list[dict] = []
        unknown_errors: list[dict] = []
        os_error: list[dict] = []
        ok_status: list[dict] = []

        batched_diagnosis = list(itertools.batched(instructions, 3))
        async with aiohttp.ClientSession() as session:
            for instructions in batched_diagnosis:
                await asyncio.sleep(1.1)
                tasks = [asyncio.create_task(self.update_product_slave(session,dicts)) for dicts in instructions]
                responses = await asyncio.gather(*tasks)
                
                #ERROR HANDLING
                for resp in responses:
                    
                    if "decoded_resp" in resp.keys():
                        if "error" in resp["decoded_resp"].keys():
                            if resp["decoded_resp"]["errror"]["type"] == "RESOURCE_NOT_FOUND":
                                error_not_found.append(resp)
                                continue
                            unknown_errors.append(resp)
                    
                    if resp["response_status"] == "OSERROR":
                        os_error.append(resp)
                    elif resp["response_status"] == 200:
                        ok_status.append(resp)

        logger.info(f"(update_product_main) PUT REQUESTS FINALIZADAS, RESULTADOS:\n \
                    error_not_found:{len(error_not_found)}\n \
                    unknown_errors:{len(unknown_errors)}\n \
                    os_errors: {len(os_error)}\n")
        if error_not_found or unknown_errors or os_error:
            await asyncio.create_task(self.error_handler(semaphore=self.semaphore ,function="update_product_main" ,
                                                         error_not_found=error_not_found, unknown_errors=unknown_errors,
                                                         os_error=os_error))
        print("(update_product_main) Atualização feita com sucesso.")

class IOHandler(ApiFunctions):

    def __init__(self):
        self.api_product_instructions = []
        self.api_stock_instructions = []
        self.yes_stockId = []
        self.no_stockId = []
        self.semaphore = asyncio.Semaphore(1)

    def verify_input(self, diagnosis):

        print("(verify_input) Separando diagnóstico")
        logger.info("(verify_input) Separando diagnóstico")
        for product in diagnosis:
            keys = [key for key in product["divergencias"].keys()]
            if "estoque" in keys or "custo" in keys:
                self.api_stock_instructions.append(product)
            if "descricao" in keys or "preco" in keys:
                self.api_product_instructions.append(product)
        
        if self.api_stock_instructions:
            for product in self.api_stock_instructions:
                if product["id_estoque"]:
                    self.yes_stockId.append(product)
                    continue
                self.no_stockId.append(product)

        logger.debug(f"self.api_product_instructions: \n{self.api_product_instructions}")
        logger.debug(f"self.api_stock_instructions: \n{self.api_stock_instructions}")
        print("(verify_input) Diagnóstico separado")
        logger.info("(verify_input) Diagnóstico separado")

    async def call_api(self):

        print("(call_api) Executando funções apropriadas")
        logger.info("(call_api) Executando funções apropriadas")
        if self.api_product_instructions:
            print("(call_api) Executando 'update_product_main'")
            logger.info("(call_api) Executando 'update_product_main'")
            await asyncio.create_task(self.update_product_main(self.api_product_instructions))
        
        if self.yes_stockId:
            logger.info("(call_api) executando 'update_stock_main'")
            logger.debug(f"self.yes_stockId: \n{self.yes_stockId}")
            await asyncio.create_task(self.update_stock_main(self.yes_stockId))
        if self.no_stockId:
            logger.info("(call_api) executando 'create_stock_main'")
            logger.debug(f"self.no_stockId: \n{self.no_stockId}")
            await asyncio.create_task(self.create_stock_main(self.no_stockId))

    def main(self):
        asyncio.run(self.call_api())