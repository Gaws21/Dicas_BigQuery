#importar biblioteca bigquery
from google.cloud import bigquery


#criar objeto cliente
"""
    Para utilizar a API em sua maquina local é preciso usar uma chave de autenticação.
    Para  isso, siga os passos em:
    https://cloud.google.com/docs/authentication/getting-started#auth-cloud-implicit-python

    Passe o caminho de sua chave json como parâmetro para o método from_service_account_json
"""
client = bigquery.Client.from_service_account_json("/home/asus/Documentos/ADS/2020/venv_bq_py3.7/venv/key_service_account_create_1.json")

"""
    Instanciar um objeto ExternalConfig do tipo CSV.
    Por default, os objetos ExternalConfig são CSV.
    
    Seguindo o mandamentos do Zen Python:
      Explícito é melhor que implícito.
"""
table_ext_clear = bigquery.external_config.ExternalConfig("CSV")

"""
    Por padrão, arquivos csv são delimitados por ponto e virgula.
    Porém é comum se deparar com arquivos com extensão .csv com outros tipos de delimitadores.
    Em especial, arquivos exportados do Excel para csv são automaticamente atribuídos ponto e virgula
    como o tipo de delimitador. 
    Caso não for configurado outro tipo específico pelo usuário, virgula será atribuído por defaut.
"""
#setar o atributo field_delimiter do arquivo externo
table_ext_clear.options.field_delimiter = ";"

"""
    Esse atributo permite que ao se executar a consulta, caso existe linhas com problemas,  
    então serão ignoradas. 
    No caso do arquivo utilizado, existiam algumas linhas que geravam erro ao executar uma consulta.
    LEMBRA-SE:Ignorar linhas é perder registros. O melhor a se fazer é corrigir seus dados externos
    na origem.
"""

#setar um valor baixo de preferência. 
table_ext_clear.max_bad_records = 10

"""
    No nosso exemplo, por questões didáticas, estamos utilizando o Google Drive.
    Porém, caso fosse utilizado Cloud Storage, o link pode ser obtido acessando
    a interface gráfica e copiando a uri fornecida do objeto.
"""

#setar a uri onde seus dados estão armazenados.
table_ext_clear.source_uris = "https://drive.google.com/open?id=1qBntr4-o7G3nY4_OomLoNdA7EUrV00nG"

"""
     Existem diferentes maneiras de expecificar um schema.
     No nosso exemplo utilizados o schema da nossa primeira tabela que apresentou problemas
     em certas linhas.
"""

tb_ext_origem = client.get_table("dataset1-261923.set1.table_ext_origem")

schema = tb_ext_origem.schema

table_ext_clear.schema = schema

#criar o id de destino da sua tabela. Composto por: nome_projeto.nome_dataset.nome_tabela
table_id = "dataset1-261923.set1.table_ext_clear"

#instanciar um objeto do tipo Table, inicializado com o id da tabela de destino.
table = bigquery.Table(table_id)

#client.delete_table(table)

#setar o atributo external_data_configuration com o objeto ExternalConfig criado na linha 23.
table.external_data_configuration = table_ext_clear

#utlizar o método create_table passando como parâmetro o objeto table.
table_created = client.create_table(table)  # Make an API request.

#print as informações da tabela criada.
print(
    "Created table {}.{}.{}".format(table_created.project, table_created.dataset_id, table_created.table_id)
)
