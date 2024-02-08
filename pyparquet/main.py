import pandas as pd

def show_parquet(path):
    df = pd.read_parquet(path)
    print(df)
# Criando um DataFrame de exemplo
# data = {
#     'coluna1': [1, 2, 3],
#     'coluna2': ['a', 'b', 'c']
# }
# df = pd.DataFrame(data)

# df.to_parquet('example.parquet')


# df_lido = pd.read_parquet('example.parquet')
# print(df_lido)

show_parquet('./../storage/example.parquet')