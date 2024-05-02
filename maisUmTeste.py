import pandas as pd

# Carregue o DataFrame a partir de alguma fonte, como um arquivo CSV
# Substitua "file_path" pelo caminho do seu arquivo ou use outra fonte de dados
df = pd.read_csv("novoBolsaFamilia_DF.csv")

# Especifique a coluna pela qual você deseja encontrar os maiores valores
coluna_especifica = "VALOR PARCELA"

# Converta a coluna para o tipo numérico
df[coluna_especifica] = pd.to_numeric(df[coluna_especifica], errors='coerce')

# Ordena o DataFrame em ordem decrescente com base na coluna especificada
df_sorted = df.sort_values(by=coluna_especifica, ascending=False)

# Pegue as 100 primeiras linhass
maiores_valores = df_sorted.head(100)

# Exibe as 100 linhas que possuem os maiores valores da coluna "VALOR PARCELA"
print(maiores_valores)
