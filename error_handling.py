import asyncio
import datetime
import itertools
import logging

from request_routine import ApiFunctions

logging.basicConfig(level=logging.DEBUG, filemode="a", filename="app_logs.log", format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger("error_handling")

class ErrorHandler(ApiFunctions):
    """Essa classe tem o propósito de fazer o tratamento adequado dos erros das outras classes e métodos."""
    
    def __init__(self, txbox, db_sync_instance):
        self.db_events = db_sync_instance
        self.cursor = db_sync_instance.cursor
        self.conn = db_sync_instance.conn
        self.txbox = txbox
        super().__init__(txbox,db_sync_instance, self.conn,self.cursor)

    def displayer(self, msg):
        print(msg)
        self.txbox.insert('end', f"{msg}\n")

    async def error_return_api(self,error_list):
        """
        Este método trata dos produtos que retornaram com erro durante as chamadas de API.
        Basicamente, ele tentará fazer uma nova request. Se o produto não estiver dentro da quantidade
        de erros permitida, ele irá criar um arquivo informando que há muitos erros.

        Se a request retornar com erro, o método chamará a si mesmo para fazer mais uma tentativa. 
        Caso retorne sem erros, ele irá atualizar o Banco de dados.
        """
        products_api_estoque = []
        products_api_produtos = []
        self.displayer("ErrorHandler - (error_return_api) produtos retornaram das requests com erro.")
        logger.warn(f"(error_return_api) {len(error_list)} produtos retornaram das requests com erro.")
        for product in error_list:
            self.cursor.execute("UPDATE db_sistema_intermediador SET internal_error_count = internal_error_count + 1 WHERE codigo_carbrasil = %s", (product["codigo_carbrasil"],))

            if product["internal_error_count"] > 2:
                self.displayer(f"ErrorHandler - (error_return_api) O seguinte produto está causando muitos erros no sistema: \n{product}\n")
                logger.error(f"(error_return_api) O seguinte produto está causando muitos erros no sistema: \n{product}\n")
                with open("ERRO_SISTEMA_INTEGRADOR.txt", "a+") as file:
                    file.write(f"{datetime.datetime.now().strftime("%d/%m/%Y %H:%M:%S")} O produto a seguir está causando muitos erros no sistema:\n")
                    file.write(f"{product}")

            on_error_limit = product["internal_error_count"] <= 2
        
            if product["endpoint_correto"] == "estoque" and on_error_limit:
                products_api_estoque.append(product)

            elif product["endpoint_correto"] == "produto" and on_error_limit:
                products_api_produtos.append(product)

            elif product["endpoint_correto"] == "ambos" and on_error_limit:
                products_api_estoque.append(product)
                products_api_produtos.append(product)
        
        self.conn.commit()

        yes_stockId = []
        no_stockId = []
        for product in products_api_estoque:
            if product["id_estoque"]:
                yes_stockId.append(product)
                continue
            no_stockId.append(product)
        
        ok_status = []
        error_status = []
        if products_api_produtos:
            response = await asyncio.create_task(self.update_product_main(products_api_produtos))
            ok_status.append(response[0])
            error_status.append(response[1])
        if yes_stockId:
            response = await asyncio.create_task(self.update_stock_main(yes_stockId))
            ok_status.append(response[0])
            error_status.append(response[1])
        if no_stockId:
            response = await asyncio.create_task(self.create_stock_main(no_stockId))
            ok_status.append(response[0])
            error_status.append(response[1])
        
        flat_error = list(itertools.chain.from_iterable(error_status))
        flat_ok = list(itertools.chain.from_iterable(ok_status))
        if flat_error:
            self.displayer(f"ErrorHandler - (error_return_api) Ainda houveram {len(flat_error)} registros que retornaram com erro. Por favor verifique os códigos.\n")
            logger.error(f"(error_return_api) Ainda houveram {len(flat_error)} registros que retornaram com erro. Por favor verifique os códigos.\n")
            for x in flat_error:
                self.displayer(f"Código com erro: {x}")
                logger.error(f"Código com erro: {x}")
            await asyncio.create_task(self.error_return_api(flat_error))
        if flat_ok:
            self.displayer(f"ErrorHandler - (error_return_api) Foram corrigidos {len(flat_ok)} códigos")
            logger.debug(f"(error_return_api) Foram corrigidos {len(flat_ok)} códigos")
            self.db_events.update_mysql_db(flat_ok)

