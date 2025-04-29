# Colunas-distintas-
Engenharia de dados

import pandas as pd
import psycopg2
import datetime

caminho_do_arquivo = r"C:\CURSO-PYTHON\PYTHON - SGBDS - arquivos\Origem de dados\V_OCORRENCIAAMPLA.json.txt"
df = pd.read_json(caminho_do_arquivo, encoding='utf-8-sig')

colunas = ["CodigoAeronave", "DataValidade", "Operador", "CPFCNPJ", "TipoUso", "Fabricante", "Modelo"]
df = df[colunas]

df.head(1)

# Parâmetros de conexão
dbname   = 'python'
user     = 'postgres'
password = 'noah'
host     = 'localhost'
port     = '5435' 

# Criar uma conexão
conexao = psycopg2.connect(dbname=dbname,user=user,password=password,host=host, port=port)
cursor = conexao.cursor() # Criar um cursor deixa manipular os dados

#delete base antes da carga
cursor.execute( " delete from public.mapeamento " )

for indice, coluna_df in df.iterrows():
    cursor.execute( """ insert into mapeamento (
                   Aeronave,
                   DataValidade,
                   Operador,
                   CPFCNPJ,
                   TipoUso,
                   Fabricante, 
                   Modelo
                   ) VALUES (%s,%s,%s,%s,%s,%s,%s)
                   
                   """ ,(
                        str(coluna_df["CodigoAeronave"])[:30],  # Ajuste para o tamanho do campo
                        coluna_df["DataValidade"],               # Supondo que já está no formato correto
                        str(coluna_df["Operador"])[:100],
                        str(coluna_df["CPFCNPJ"])[:20],
                        str(coluna_df["TipoUso"])[:50],
                        str(coluna_df["Fabricante"])[:50],
                        str(coluna_df["Modelo"])[:50]
                       
                   )
                   )

# Depois do for todo:
conexao.commit()
cursor.close()
conexao.close()

