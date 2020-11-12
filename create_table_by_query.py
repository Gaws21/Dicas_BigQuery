from google.cloud import bigquery

#Construa/instancie um objeto cliente do BigQuery
client = bigquery.Client.from_service_account_json("/home/asus/Documentos/ADS/2020/venv_bq_py3.7/venv/key_service_account_create_1.json")

#cri um id para sua tabela destino no formato nome_projeto.nome_dataset.nome_tabela
table_id = "dataset1-261923.set1.table_inter_transformed"

#criar um job_config e set o argumento destination com o id criado anteriormente
job_config = bigquery.QueryJobConfig(destination=table_id)

#reescrever tabela
#https://googleapis.dev/python/bigquery/latest/generated/google.cloud.bigquery.job.QueryJobConfig.html#google.cloud.bigquery.job.QueryJobConfig.write_disposition

#setar atributo write_disposition para permitir replace na tabela.
job_config.write_disposition="WRITE_TRUNCATE"


#Função responsável por pela limpeza nas descrições das moedas
udf = """create temp function replace_latin_1(entrada string) AS 
(regexp_replace(
	regexp_replace(
		regexp_replace(
			regexp_replace(
				regexp_replace(
					regexp_replace(
						regexp_replace(
							regexp_replace(
								regexp_replace(
									regexp_replace(
										regexp_replace(
											regexp_replace(
												regexp_replace(
													regexp_replace(entrada,
													r"[ÀÁÂÃÄÅÆ]","A"),
												r"[ÈÉÊË£]","E"),
											r"[ÌÍÎÏ]","I"),
										r"[Ð]","D"),
									r"[Ñ]","N"),
								r"[ÒÓÔÕÖ]","O"),
							r"[Ø]","0"),
						r"[ÙÚÛÜ]","U"),
					r"[Ý]","Y"),
				r"[Þ]","P"),
			r"[ß]","B"),
		r"[¢Ç]","C"),
	r"[\\",^`~¡¢£¤¥¦§¨©ª«¬®¯°±²³´µ¶·¸¹º»¼½¾¿]",""),
r"[×]","X")
);
"""
"""
    Nessa query utilizamos função de CTE - Comum Tamble  Expressions.
    Eu particularmente gosto de utilizá-las, facilidando futura
    reutilização do código, alterações melhorando a leitura do código.
"""
query = """
    WITH table_local_source_transformed AS (
	SELECT 
	PARSE_DATE("%Y-%m-%d", data) DATA, 
	EXTRACT(YEAR FROM PARSE_DATE("%Y-%m-%d", data)) AS ANO, 
	EXTRACT(QUARTER FROM PARSE_DATE("%Y-%m-%d", data)) AS TRIMESTRE, 
	EXTRACT(MONTH FROM PARSE_DATE("%Y-%m-%d", data)) AS MES, 
	EXTRACT(WEEK FROM PARSE_DATE("%Y-%m-%d", data)) AS SEMANA, 
	EXTRACT(DAYOFWEEK FROM PARSE_DATE("%Y-%m-%d", data)) AS DIA_DA_SEMANA,
	replace_latin_1(UPPER(descricao)) AS DESC_MOEDA_CEDULA,
	SAFE_CAST(valor AS NUMERIC) MOEDA_CEDULA, 
	SAFE_CAST(REPLACE(REPLACE(quantidade,".", ""),",",".") AS NUMERIC) QTD_CIRCULACAO
	FROM `dataset1-261923.set1.table_local_source`)

    SELECT * FROM table_local_source_transformed
"""

"""
    Por fim é ncessário concatenar as partes e formar única query final.
"""
query_final = udf + query


"""
Aplique o metodo .query passando com parâmetro a query_final e setando o o argumento
job_config com o objeto job_config criado anteriormente.
"""

#criar tabela
query_job = client.query(query_final, job_config=job_config) 
query_job.result()  # Wait for the job to complete.

print("Query results loaded to the table {}".format(table_id))