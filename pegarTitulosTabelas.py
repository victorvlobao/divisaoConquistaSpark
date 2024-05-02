import csv

def print_column_titles(csv_file):
    with open(csv_file, 'r', newline='', encoding='latin1') as file:
        reader = csv.reader(file)
        column_titles = next(reader)  # Obtém a primeira linha do arquivo CSV, que contém os títulos das colunas
        print(column_titles)

# Substitua 'seu_arquivo.csv' pelo caminho do seu arquivo CSV
print_column_titles('NovoBolsaFamilia.csv')
