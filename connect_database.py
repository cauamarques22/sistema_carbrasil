import pymysqlpool
import pymysql
import pyodbc

try:
    config = {"user":"root", "password":"123456", "database":"intermediador_bling", "host":"CARBRASIL-HOST"}
    conn_pool = pymysqlpool.ConnectionPool(name="sistema_integrador", size=5,maxsize=8, **config) 
    conn1 = conn_pool.get_connection()
    cursor1 = conn1.cursor()
    cursor1.execute("SELECT * FROM db_sistema_intermediador")
except pymysql.err.OperationalError as err:
    if err.errno == 1049:
        config = {"user":"root", "password":"123456", "database":"intermediador_bling", "host":"CARBRASIL-HOST"}
        conn_pool = pymysqlpool.ConnectionPool(name="sistema_integrador", size=5,maxsize=8, **config)            
        cursor1 = conn1.cursor()
        cursor1.execute("CREATE DATABASE intermediador_bling")
        cursor1.execute("USE intermediador_bling")
        cursor1.execute("CREATE TABLE db_sistema_intermediador(codigo_carbrasil INT PRIMARY KEY, id_bling BIGINT, idEstoque BIGINT,\
                    descricao VARCHAR(255), preco FLOAT(8,3), custo FLOAT(8,3), estoque INT, bling_tipo VARCHAR(1), bling_formato VARCHAR(1), \
                    bling_situacao VARCHAR(1), gtin VARCHAR(50), peso_liquido FLOAT, peso_bruto FLOAT, marca VARCHAR(255), largura FLOAT, \
                    altura FLOAT, profundidade FLOAT)")
        conn1.commit()
    elif err.errno == 1146:
        cursor1.execute("CREATE TABLE db_sistema_intermediador(codigo_carbrasil INT PRIMARY KEY, id_bling BIGINT, idEstoque BIGINT,\
                    descricao VARCHAR(255), preco FLOAT(8,3), custo FLOAT(8,3), estoque INT, bling_tipo VARCHAR(1), bling_formato VARCHAR(1), \
                    bling_situacao VARCHAR(1), gtin VARCHAR(50), peso_liquido FLOAT, peso_bruto FLOAT, marca VARCHAR(255), largura FLOAT, \
                    altura FLOAT, profundidade FLOAT)")
        conn1.commit()
    else:
        raise err
finally:
    cursor1.fetchall()
    conn1.close()


cnxn_str = ("Driver={SQL SERVER};"
            "Server=CARBRASIL-HOST\\SQLEXPRESS;"
            "Database=bdados1;"
            "UID=sa;"
            "PWD=Tec12345678;")

def get_cb_connection():
    return pyodbc.connect(cnxn_str)

