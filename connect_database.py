import mysql.connector
import pyodbc

class DatabaseConnection():
    def __init__(self):
        self.connect_mysql()
        self.cb_connection = None

    def connect_mysql(self):
        try:
            self.conn_pool = mysql.connector.pooling.MySQLConnectionPool(pool_name="sistema_integrador", pool_size=4, user="root", password="123456", database="intermediador_bling", host="CARBRASIL-HOST") 
            self.conn1 = self.conn_pool.get_connection()
            self.cursor1 = self.conn1.cursor()
            self.cursor1.execute("SELECT * FROM db_sistema_intermediador")
        except mysql.connector.ProgrammingError as err:
            if err.errno == 1049:
                self.conn1 = mysql.connector.pooling.MySQLConnectionPool(pool_name="sistema_integrador", pool_size=4,user="root", password="123456", host="CARBRASIL-HOST")            
                self.cursor1 = self.conn1.cursor()
                self.cursor1.execute("CREATE DATABASE intermediador_bling")
                self.cursor1.execute("USE intermediador_bling")
                self.cursor1.execute("CREATE TABLE db_sistema_intermediador(codigo_carbrasil INT PRIMARY KEY, id_bling BIGINT, idEstoque BIGINT,\
                            descricao VARCHAR(255), preco FLOAT(8,3), custo FLOAT(8,3), estoque INT, bling_tipo VARCHAR(1), bling_formato VARCHAR(1), \
                            bling_situacao VARCHAR(1), gtin VARCHAR(50), peso_liquido FLOAT, peso_bruto FLOAT, marca VARCHAR(255), largura FLOAT, \
                            altura FLOAT, profundidade FLOAT)")
                self.conn.commit()
            if err.errno == 1146:
                self.cursor1.execute("CREATE TABLE db_sistema_intermediador(codigo_carbrasil INT PRIMARY KEY, id_bling BIGINT, idEstoque BIGINT,\
                            descricao VARCHAR(255), preco FLOAT(8,3), custo FLOAT(8,3), estoque INT, bling_tipo VARCHAR(1), bling_formato VARCHAR(1), \
                            bling_situacao VARCHAR(1), gtin VARCHAR(50), peso_liquido FLOAT, peso_bruto FLOAT, marca VARCHAR(255), largura FLOAT, \
                            altura FLOAT, profundidade FLOAT)")
                self.conn1.commit()

    def connect_carbrasil(self):
        cnxn_str = ("Driver={SQL SERVER};"
                        "Server=CARBRASIL-HOST\\SQLEXPRESS;"
                        "Database=bdados1;"
                        "UID=sistema_integrador;"
                        "PWD=cb010306;")

        self.cb_connection = pyodbc.connect(cnxn_str)
        
        #cb_cursor = None
