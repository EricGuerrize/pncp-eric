import os
import oracledb
import pandas as pd
from dotenv import load_dotenv

# Carrega as variáveis do arquivo .env
load_dotenv()

def extrair_dados_oracle(query: str, params: dict = None) -> pd.DataFrame:
    """
    Conecta ao Oracle e retorna os resultados de uma query como DataFrame do Pandas.
    Usa o modo "Thin" da biblioteca oracledb, que não exige instalação de Oracle Client na máquina.
    """
    
    # Pega as credenciais do ambiente (.env)
    user = os.getenv("ORACLE_USER")
    password = os.getenv("ORACLE_PASSWORD")
    dsn = os.getenv("ORACLE_DSN")

    if not all([user, password, dsn]):
        raise ValueError("Credenciais Oracle ausentes. Faça uma cópia do arquivo '.env.example' "
                         "para '.env' e preencha ORACLE_USER, ORACLE_PASSWORD e ORACLE_DSN.")

    conn_info = {
        "user": user,
        "password": password,
        "dsn": dsn
    }

    try:
        print("Conectando ao banco Oracle...")
        # Conecta ao banco
        connection = oracledb.connect(**conn_info)
        print("Conexão bem-sucedida! Executando a extração...")
        
        # Executa a query e carrega para o pandas 
        # Passar params previne SQL Injection e é a forma correta com o pandas
        df = pd.read_sql(query, connection, params=params)
        
        print(f"Extração concluída: {len(df)} registros encontrados.")
        return df

    except Exception as e:
        print(f"Ocorreu um erro ao conectar ou consultar o Oracle:\n{e}")
        return pd.DataFrame() # Retorna DF vazio para não quebrar pipelines
    finally:
        if 'connection' in locals() and connection:
            connection.close()
            print("Conexão com o Oracle fechada.")

if __name__ == "__main__":
    # ----------------------------------------------------------------------------------
    # EXEMPLO DE USO
    # Para testar este script isoladamente, coloque as senhas no arquivo .env
    # e descomente o código abaixo para exportar os dados para CSV ou Excel automaticamente.
    # ----------------------------------------------------------------------------------
    
    query_teste = """
    SELECT CHR(15712167) || P.ENT_CODIGO AS "Cód. UG",
           VW.NOME_entidade AS "UG"
    FROM aplic2008.PROCESSO_LICITATORIO@conectprod P
    INNER JOIN aplic2008.ENTIDADE@conectprod VW ON P.ENT_CODIGO = VW.CNPJ_CPF_COD_TCE_ENTIDADE
    WHERE ROWNUM <= 5
    """
    
    df = extrair_dados_oracle(query_teste)
    
    if not df.empty:
        print(df.head())
        # Salva como CSV em UTF-8 forçando o BOM (byte_order_mark) 
        df.to_csv("teste_extracao.csv", index=False, encoding='utf-8-sig')
        print("Arquivo salvo como 'teste_extracao.csv'")
