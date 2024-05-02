import csv

def transform_column_to_float(csv_file, column_index):
    """
    Função para ler um arquivo CSV e converter uma coluna de string para float.
    
    Parâmetros:
    csv_file (str): Caminho para o arquivo CSV.
    column_index (int): Índice da coluna a ser convertida para float (0-based index).
    
    Retorna:
    list: Lista dos valores da coluna convertida para float.
    """
    column_float = []

    with open(csv_file, 'r') as file:
        reader = csv.reader(file)
        for row in reader:
            try:
                value = float(row[column_index])
            except ValueError:
                # Se a conversão falhar, insira None na lista
                value = None
            column_float.append(value)

    return column_float

# Exemplo de uso:
csv_file = 'novoBolsaFamilia_DF.csv'
column_index = 2  # Índice da coluna a ser convertida para float
column_float = transform_column_to_float(csv_file, column_index)
print(column_float)
