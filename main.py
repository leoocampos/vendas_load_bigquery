import os
import io
import pandas as pd
import pytz
import logging
from datetime import datetime
from flask import Flask, request, jsonify
from google.cloud import storage, bigquery

app = Flask(__name__)

# Configurações
TIME_SP = 'America/Sao_Paulo'
DATASET_ID = 'BRONZE'
TABLE_ID = 'vendas'
BUCKET_NAME = "sample-track-files"

@app.post('/vendas_load_bigquery')
def load_vendas_to_bq():
    try:
        # 1. Obter nome do arquivo do JSON enviado (ou fixo se preferir)
        data = request.get_json()
        file_name = data.get('file_name', 'vendas.xlsx')
        bucket_name = BUCKET_NAME

        # 2. Conectar aos Clientes
        storage_client = storage.Client()
        bq_client = bigquery.Client()
        
        # 3. Ler arquivo do Cloud Storage para a memória
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(file_name)
        content = blob.download_as_bytes()

        # 4. Processar com Pandas (Lendo os bytes do Excel)
        df_vendas = pd.read_excel(io.BytesIO(content), engine='openpyxl')
        
        # Adicionar coluna de data de carga
        dat_ref = datetime.now(tz=pytz.timezone(TIME_SP)).date()
        df_vendas = df_vendas.assign(dat_ref_carga=pd.to_datetime(dat_ref))

        # 5. Carregar no BigQuery
        # table_id completo: projeto.dataset.tabela
        table_full_id = f"{bq_client.project}.{DATASET_ID}.{TABLE_ID}"
        
        # Configuração da carga (Append para não apagar o que já existe)
        job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
        
        job = bq_client.load_table_from_dataframe(df_vendas, table_full_id, job_config=job_config)
        job.result()  # Espera a carga terminar

        logging.info(f"Carga concluída: {len(df_vendas)} linhas inseridas em {table_full_id}")
        
        return jsonify({"status": "success", "rows": len(df_vendas)}), 200

    except Exception as e:
        logging.error(f"Erro na carga: {e}")
        return jsonify({"status": "error", "details": str(e)}), 500

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))