import pandas as pd
from google.cloud import bigquery
import basedosdados as bd
from tqdm import tqdm

def count_null_values(dataset_id: str, table_id: str, project_id: str) -> pd.DataFrame:
    """
    Conta a frequência de valores nulos de cada coluna da tabela, assim como o total de linhas. Caso a tabela
    possua a coluna ano, o valor de nulos e de total de linhas será dividio por ano.

    Args:
        dataset_id (str): ID do dataset no BigQuery.
        table_id (str): ID da tabela no BigQuery.
        project_id (str): ID (billing id) do seu projeto no BigQuery.
    
    Returns:
        pd.DataFrame: DataFrame do Pandas.
    """
    
    # Buscando os nomes, types e descriptions das colunas
    schema = bd.get_table_columns(dataset_id, table_id, verbose=False)
    
    # Inicializando o dataframe que irá armazenar o número de valores nulos em cada coluna
    null_counts = pd.DataFrame()
    df2 = pd.DataFrame()

    # Caso a tabela possua a coluna ano, o número de nulos e o tamanho da tabela será dado por ano
    if 'ano' in [x.get('name') for x in schema]:
        query = f"SELECT ano, COUNT(*) FROM `basedosdados.{dataset_id}.{table_id}` GROUP BY ANO"
        query_job = bd.read_sql(query, billing_project_id = project_id)
        
        for year in tqdm(range(len(query_job.ano))):
            for field in [i['name'] for i in schema]:
                print(f'Iniciando o ano {year}')
                null_counts['table_size'] = pd.Series(query_job.iloc[year][1])
                null_counts['ano'] = pd.Series(query_job.iloc[year][0])
                # Fazendo uma consulta SQL para contar o número de valores nulos na coluna
                query2 = f"SELECT COUNT(*) FROM `basedosdados.{dataset_id}.{table_id}` WHERE {field} IS NULL AND ano = {query_job.iloc[year][0]}"
                query_job2 = bd.read_sql(query2, billing_project_id='casebd')

                # Obtendo o resultado da consulta e adicionando o número de valores nulos ao dataframe
                null_count = pd.Series(query_job2.iloc[0][0])
                null_counts[field] = null_count
                #print(null_counts)
            df2 = df2.append(null_counts, ignore_index= True)

    else:
        query = f"SELECT COUNT(*) FROM `basedosdados.{dataset_id}.{table_id}`"
        query_job = bd.read_sql(query, billing_project_id='casebd')
        
        for field in [i['name'] for i in schema]:
            null_counts['table_size'] = pd.Series(query_job.iloc[0][0])
            # Fazendo uma consulta SQL para contar o número de valores nulos na coluna
            query2 = f"SELECT COUNT(*) FROM `basedosdados.{dataset_id}.{table_id}` WHERE {field} IS NULL"
            query_job2 = bd.read_sql(query2, billing_project_id='casebd')
            # Obtendo o resultado da consulta e adicionando o número de valores nulos ao dataframe
            null_count = pd.Series(query_job2.iloc[0][0])
            null_counts[field] = null_count
        df2 = df2.append(null_counts, ignore_index= True)
        
   
    return df2



def check_column_types(dataset_id: str, table_id: str, architecture_path: str) -> pd.DataFrame:
    """
    Checa o tipo de cada coluna no BigQuery e compara com a arquitetura.

    Args:
        dataset_id (str): ID do dataset no BigQuery.
        table_id (str): ID da tabela no BigQuery.  
        architecture_path (str): Caminho para a tabela de arquitetura em .csv.
    Returns:
        pd.DataFrame: DataFrame do Pandas.
    """
    # Criando um cliente do BigQuery
    # Buscando os nomes, types e descriptions das colunas
    schema = bd.get_table_columns(dataset_id, table_id, verbose=False)

    # Lendo o arquivo de arquitetura
    arq = pd.read_csv(f'{architecture_path}', dtype='string')
    arq['bigquery_type'] = arq['bigquery_type'].apply(lambda x: x.lower())
    arq = arq[['name', 'bigquery_type']].rename(columns={'name': 'name_arq', 'bigquery_type': 'bq_type_arq'})
    # Inicializando a lista que irá armazenar o nome e o tipo de cada coluna
    column_types = []

    # Iterando pelas colunas da tabela
    for field in schema:
        # Adicionando o nome e o tipo da coluna à lista
        column_types.append((field.get('name'), field.get('bigquery_type')))

    # Retornando o dataframe com o nome e o tipo de cada coluna
    df = pd.DataFrame(column_types, columns=['name_ckan', 'bq_type_ckan'], dtype='string')
    df['name_ckan'] = df['name_ckan'].apply(lambda x: x.lower())
    df = pd.merge(df, arq, how='outer', left_on= 'name_ckan', right_on='name_arq')

    ## Retornando o BigQuery type do BQ:

    a = bd.read_sql(f'SELECT * from basedosdados.{dataset_id}.INFORMATION_SCHEMA.COLUMNS', billing_project_id='casebd')
    a = a[a['table_name'] == f'{table_id}']
    a['data_type'] = a['data_type'].apply(lambda x: x.lower())
    a['column_name'] = a['column_name'].apply(lambda x: x.lower())
    a = a[['column_name', 'data_type']].rename(columns = {'column_name': 'bq_name', 'data_type': 'bq_type'})
    df = pd.merge(df, a, how='outer', left_on= 'name_ckan', right_on='bq_name')

    ## Cria coluna que checa se os nomes estão iguais entre as duas colunas
    df['match_names_arq'] = df['bq_name'] == df['name_arq']
    df['match_types_arq'] = df['bq_type'] == df['bq_type_arq']

    return df


## to-do: automatizar processo de preencher o link com um dicionário

def check_directory_link(dataset_id: str, table_id: str) -> pd.DataFrame:
    """
    Checa o link no diretorio de cada coluna.

    Args:
        dataset_id (str): ID do dataset no BigQuery.
        table_id (str): ID da tabela no BigQuery.  

    Returns:
        pd.DataFrame: DataFrame do Pandas.
    """
    # Criando um cliente do BigQuery
    # Buscando os nomes, types e descriptions das colunas
    schema = bd.get_table_columns(dataset_id, table_id, verbose=False)

    ## lista com os nomes de colunas que precisam ter links nos diretórios
    directory_vector = ['id_escola', 'id_municipio', 'sigla_uf', 'id_municipio_tse', 'id_uf', 'id_natureza_juridica', 'id_etnia_indigena', 'id_regiao_metropolitana', 'especialdiade','cbo_1994', 'cbo_2002', 'subcategoria', 'categoria',
    'cnae_1', 'cnae_2', 'id_curso', 'id_distrito','id_ies', 'id_setor_censitario', 'nome', 'sigla', 'ano', 'bimestre', 'semestre','data', 'dia', 'hora', 'mes', 'minuto', 'segundo',
    'tempo', 'trimestre']

    # Inicializando a lista que irá armazenar o nome e o tipo de cada coluna
    column_types = []

    if any(i in [x.get('name') for x in schema] for i in directory_vector):

        # Iterando pelas colunas da tabela
        for field in schema:
            # Adicionando o nome e o tipo da coluna à lista
            column_types.append((field.get('name'), field.get('directory_column').get('dataset_id'), field.get('directory_column').get('table_id'), field.get('directory_column').get('column_name')))

        # Retornando o dataframe com o nome e o tipo de cada coluna
        df = pd.DataFrame(column_types, columns=['name', 'dir_dataset_id', 'dir_table_id', 'dir_column_name'], dtype='string')

    return df