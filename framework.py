import os
import pandas
import ibm_db
import ibm_db_dbi
import datetime

class PandasBB:    
    r"""Classe criada para auxiliar leitura, cargas e validações de dados do command center
    
    Métodos
    ----------
    select_db2: 
    insert_db2:
    check_nulls: 
    check_datatypes: 
    check_max_value: 
    check_duplicates: 
    """
        
    def select_db2(uid: str, pwd: int, sql: str):
        """
        
        """
        query = f"({sql})"

        conn = ibm_db.connect(f'DATABASE=BDB2P04;HOSTNAME=gwdb2.bb.com.br;PORT=50100;PROTOCOL=TCPIP;'f'UID={uid};'f'PWD={pwd};' , '', '')

        dbi = ibm_db_dbi.Connection(conn)

        df = pandas.read_sql(query, dbi)

        return df
        
        ibm_db.close(dbi)
        
    
    def insert_db2(uid: str, pwd: int, df: pandas.DataFrame, table: str, truncate: bool=False):
        """
        Insere dados de um DataFrame pandas em uma tabela DB2.
        """
        
        try:
            conn = ibm_db.connect(f'DATABASE=BDB2P04;HOSTNAME=gwdb2.bb.com.br;PORT=50100;PROTOCOL=TCPIP;'f'UID={uid};'f'PWD={pwd};' , '', '')
            print("Conexão criada.")

            if truncate:
                truncate_query = f"TRUNCATE TABLE {table}"
                stmt = ibm_db.exec_immediate(conn, truncate_query)
                print(f"Tabela {table} truncada.")

            if isinstance(df, pandas.DataFrame):
                columns = ', '.join(df.columns)
                placeholders = ', '.join(['?' for _ in df.columns])
                query = f"INSERT INTO {table} ({columns}) VALUES ({placeholders})"       
                print(query)

                for row in df.itertuples(index=False, name=None):
                    stmt = ibm_db.prepare(conn, query)
                    ibm_db.execute(stmt, row)

                print("Dados inseridos com sucesso a partir do DataFrame.")
            elif isinstance(df, dict):
                columns = ', '.join(df.keys())
                placeholders = ', '.join(['?' for _ in df.keys()])
                query = f"INSERT INTO {table} ({columns}) VALUES ({placeholders})"
                stmt = ibm_db.prepare(conn, query)
                ibm_db.execute(stmt, tuple(df.values()))
                print("Registo único inserido com sucesso.")
            else:
                print("Tipo de dados inválido. Forneça um dicionário ou um DataFrame pandas.")

            ibm_db.close(conn)
            print("Conexão fechada.")
        except Exception as e:
            print(f"Erro: {e}")

    
    def check_nulls(df: pandas.DataFrame, columns: list):
        """
        """
        n = 0
        for col in columns:
            if df[col].isnull().any():
                print(f"Coluna {col} possui valores nulos")
                n = n+1
        print(f"{n} colunas com valores nulos...\n")
        
        
    def check_datatypes(df: pandas.DataFrame, columns: list):
        """
        """
        for col in columns:
            if df[col].dtype == 'object':
                print(f'{col}: String')
            elif df[col].dtype in [np.int64, np.float64]:
                print(f'{col}: Number')
            elif df[col].dtype == 'bool':
                print(f'{col}: Boolean')
        
                
    def check_max_value(series):
        """
        Example call:
            maxlen = df.apply(check_max_value)
        """
        return series.apply(lambda x: len(str(x))).max()
    
    
    def check_duplicates(df: pandas.DataFrame, key: list) -> pandas.DataFrame:
        """
        """
        if key_columns:
            duplicates = df[df.duplicated(subset=key, keep=False)]
        else:
            duplicates = df[df.duplicated(keep=False)]
            
        print(f"Existem {len(duplicates.index)} registros duplicados")
        
        return duplicates
    
    def order_columns(df_pre: pandas.DataFrame, df_pos: pandas.DataFrame) -> pandas.DataFrame:
        """
        """
        df = df_pre[df_pos.columns]
        
        return df
    
    
    def rename_column(df: pandas.DataFrame, column: str, renamed: str) -> pandas.DataFrame:
        """
        """
        df = df.rename(columns={f'{column}': f'{renamed}'})
        
        return df
    
    
    def percent_variation(df: pandas.DataFrame, column: str, expected: int):
        """
        """
        # Calculate percent variation
        df['VAR'] = df[f"{column}"].pct_change() * 100
        
        # Filter rows where percent_variation > expected
        outlier = df[df['VAR'] > expected]
        
        return outlier
    
    
    def max_date_exists(df1: pandas.DataFrame, df2: pandas.DataFrame, column: str):
        """
        """
        # Get the maximum date from df1
        max_date = df1[column].max()

        # Check if max_date_df1 exists in df2
        exists = max_date in df2[column].values

        return exists
    
    
    def count_by_group(df: pandas.DataFrame, column: str):
        """
        """
        grouped = df.groupby(column).size().reset_index(name='count')
        g = len(grouped.index)
        
        return g
    
    def get_timestamp(df: pandas.DataFrame) -> pandas.DataFrame:
        """
        """
        df = df.assign(timestamp=pandas.to_datetime(datetime.datetime.now()))
        
        return df
        
        
class SparkBB():
    pass