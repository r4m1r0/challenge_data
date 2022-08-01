try:
    from datetime import timedelta
    from airflow import DAG
    from airflow.operators.python_operator import PythonOperator
    from datetime import datetime
    import requests
    from urllib import response
    import gzip
    import pandas as pd
    from datetime import datetime
    import os
    import shutil
    print("Todo los DAG modulos estan bien...")
except Exception as e:
    print("Error {} ".format(e))

def download_function_execute():
    url = "https://smn.conagua.gob.mx/webservices/index.php?method=1"
    response = requests.get(url)
    open("DailyForecast_MX.gz", "wb").write(response.content)

def descompresion_function_execute():
    with gzip.open("DailyForecast_MX.gz", "rb") as f_in:
        with open( "DailyForecast_MX.json", "wb" ) as f_out:
            shutil.copyfileobj(f_in, f_out)

def read_json_function_execute():
    df = pd.read_json('DailyForecast_MX.json')
    df_fecha = df.groupby(["ides", "idmun"], as_index=False)["dloc"].max()
    df_ptmin = df.groupby(["ides", "idmun"], as_index=False)["tmin"].mean()
    df_ptmax = df.groupby(["ides", "idmun"], as_index=False)["tmax"].mean()
    df_t = pd.merge(df_ptmin, df_ptmax, how="inner", left_on=["ides", "idmun"], right_on=["ides", "idmun"])
    df_t["temp"] = (df_t["tmin"] + df_t["tmax"])/2
    df_prec = df.groupby(["ides", "idmun"], as_index=False)["prec"].mean()
    df_aux = pd.merge(df_fecha, df_t, how="inner", left_on=["ides", "idmun"], right_on=["ides", "idmun"])
    df_aux = pd.merge(df_aux, df_prec, how="inner", left_on=["ides", "idmun"], right_on=["ides", "idmun"])
    df_final = df_aux[["ides", "idmun", "dloc", "temp", "prec"]]
    path = os.path.join(os.getcwd(), "data_municipios")
    dirs = []
    for (dirpath, dirnames, filenames) in os.walk(path):
        dirs = dirs + dirnames
    last_dirs = max(dirs)
    csv_path = os.path.join(path, last_dirs, "data.csv")
    df_value = pd.read_csv(csv_path)
    df_csv = pd.merge(df_final, df_value, how="inner", left_on=["ides", "idmun"], right_on=["Cve_Ent", "Cve_Mun"])
    df_csv = df_csv.drop(['Cve_Ent', 'Cve_Mun', "dloc"], axis=1)
    df_csv.rename(columns = {'ides':'Cve_Ent', 'idmun':'Cve_Mun'}, inplace = True)
    fechaact = datetime.now()
    yr = fechaact.year
    mt = ('00' + str(fechaact.month))[-2:]
    dy = ('00' + str(fechaact.day))[-2:]
    fecha_actual = f'{yr}{mt}{dy}'
    path_new_folder = os.path.join(path, fecha_actual)
    try:
        os.mkdir(path_new_folder)
    except FileExistsError:
        shutil.rmtree(path_new_folder)
        os.mkdir( path_new_folder )
    df_csv.to_csv(os.path.join(path_new_folder, 'data.csv'), index=False)

with DAG(
    dag_id="first-dag",
    schedule_interval="@daily",
    default_args={
        "owner": "airflow",
        "retries": 1,
        "retry_delay": timedelta(minutes=60),
        "start_date": datetime(2020, 7, 28),
    },
    catchup=False) as f:

    download_function_execute = PythonOperator(
        task_id="download_function_execute",
        python_callable=download_function_execute)

    descompresion_function_execute = PythonOperator(
        task_id="descompresion_function_execute",
        python_callable=descompresion_function_execute)

    read_json_function_execute = PythonOperator(
        task_id="read_json_function_execute",
        python_callable=read_json_function_execute)

download_function_execute >> descompresion_function_execute >> read_json_function_execute