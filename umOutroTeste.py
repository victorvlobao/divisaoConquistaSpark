import pandas as pd
from pyspark.sql import SparkSession

# Função para imprimir CPF e Nome das pessoas
def imprimir_maiores_beneficiarios(df):
    for index, row in df.iterrows():
        print(f"CPF: {row['CPF']}, Nome: {row['NOME']}")

# Carregar dados do arquivo CSV
dados = pd.read_csv("NovoBolsaFamilia.csv", encoding='latin1', sep=';')

# Verificar se a coluna 'UF' está presente nos dados
if 'UF' not in dados.columns:
    raise ValueError("A coluna 'UF' não foi encontrada nos dados.")

# Verificar se a coluna 'Valor' está presente nos dados
if 'VALOR PARCELA' not in dados.columns:
    raise ValueError("A coluna 'VALOR PARCELA' não foi encontrada nos dados.")

# Filtrar dados por UF
dados_por_uf = {}
for uf, grupo in dados.groupby('UF'):
    dados_por_uf[uf] = grupo

# Salvar dados filtrados em arquivos CSV separados por UF
for uf, df in dados_por_uf.items():
    df.to_csv(f'novoBolsaFamilia_{uf}.csv', index=False)

# Calcular total de benefícios por UF
total_por_uf = {}
for uf, df in dados_por_uf.items():
    total_por_uf[uf] = df['VALOR PARCELA'].sum()

# Identificar os 100 maiores beneficiários por UF
maiores_beneficiarios_por_uf = {}
for uf, df in dados_por_uf.items():
    maiores_beneficiarios_por_uf[uf] = df.nlargest(100, 'VALOR PARCELA')

# Salvar resultados em arquivos CSV
for uf, df in maiores_beneficiarios_por_uf.items():
    df.to_csv(f'maiores_beneficiarios_{uf}.csv', index=False)


# Iniciar sessão Spark
spark = SparkSession.builder \
    .appName("BolsaFamiliaAnalysis") \
    .getOrCreate()

# Carregar dados CSV em DataFrame Spark
df = spark.read.csv('novoBolsaFamilia.csv', header=True, sep=';')

# Filtrar dados por UF
ufs = df.select('UF').distinct().rdd.flatMap(lambda x: x).collect()

# Calcular total de benefícios por UF
total_por_uf = {}
for uf in ufs:
    total_por_uf[uf] = df.filter(df.UF == uf).agg({"VALOR PARCELA": "sum"}).collect()[0][0]

# Identificar os 100 maiores beneficiários por UF
maiores_beneficiarios_por_uf = {}
for uf in ufs:
    maiores_beneficiarios_por_uf[uf] = df.filter(df.UF == uf).orderBy(df['VALOR PARCELA'].desc()).limit(100)

# Salvar resultados em arquivos CSV usando o Spark
for uf, df in maiores_beneficiarios_por_uf.items():
    df.write.csv(f'maiores_beneficiarios_{uf}.csv', header=True, mode='overwrite')

# Imprimir CPF e Nome dos maiores beneficiários por UF
for uf, df in maiores_beneficiarios_por_uf.items():
    print(f"\nMaiores beneficiários em {uf}:")
    imprimir_maiores_beneficiarios(df)
