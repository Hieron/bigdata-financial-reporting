import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lag, avg
from pyspark.sql.window import Window
from datetime import datetime

def validate_date_format(date_string):
    """
    Valida se a string está no formato 'yyyy-MM-dd'.
    """
    try:
        datetime.strptime(date_string, "%Y-%m-%d")
        return True
    except ValueError:
        return False

def read_data(spark, hdfs_input_dataset_path):
    """
    Lê o dataset do HDFS e retorna um DataFrame do Spark.
    """
    try:
        df = spark.read \
            .format("csv") \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .option("sep", ",") \
            .load(hdfs_input_dataset_path)
        return df
    except Exception as e:
        print(f"Erro ao ler o dataset do HDFS: {e}")
        sys.exit(-1)

def calculate_daily_returns(df, initial_date, final_date):
    """
    Filtra os dados entre as datas especificadas e calcula os retornos diários.
    """
    try:
        filtered_df = df.filter((col("Date") >= initial_date) & (col("Date") <= final_date))
        window_spec = Window.orderBy("Date")
        
        daily_returns = filtered_df.withColumn(
            "DOLAR_Retorno", (col("DOLAR") / lag("DOLAR").over(window_spec) - 1) * 100
        ).withColumn(
            "S&P500_Retorno", (col("S&P500") / lag("S&P500").over(window_spec) - 1) * 100
        )
        
        return daily_returns
    except Exception as e:
        print(f"Erro ao calcular os retornos diários: {e}")
        sys.exit(-1)

def save_to_hdfs(df, path, description):
    """
    Salva o DataFrame no HDFS como CSV.
    """
    try:
        df.coalesce(1) \
            .write \
            .mode("overwrite") \
            .option("header", "true") \
            .csv(path)
        print(f"{description} salvo com sucesso em {path}.")
    except Exception as e:
        print(f"Erro ao salvar {description} no HDFS: {e}")
        sys.exit(-1)

def main(initial_date, final_date, job_id, dataset_path):
    # Validar formato das datas
    if not validate_date_format(initial_date) or not validate_date_format(final_date):
        print("Formato de data inválido. Use o formato 'yyyy-MM-dd'.")
        sys.exit(-1)

    # Caminhos dos dados no HDFS
    hdfs_input_dataset_path = f"hdfs://coordinator:9000{dataset_path}"
    hdfs_output_daily_returns_path = f"hdfs://coordinator:9000/output/{job_id}/daily_returns"
    hdfs_output_average_daily_return_path = f"hdfs://coordinator:9000/output/{job_id}/average_daily_return"

    try:
        # Inicializar a sessão Spark
        spark = SparkSession.builder \
            .appName("Market Data Analysis") \
            .master("spark://coordinator:7077") \
            .getOrCreate()

        # Ler os dados do HDFS
        df = read_data(spark, hdfs_input_dataset_path)
        df = df.fillna(0)  # Substituir valores nulos por 0
        
        # Calcular os retornos diários
        daily_returns = calculate_daily_returns(df, initial_date, final_date)
        
        # Salvar os retornos diários no HDFS
        save_to_hdfs(daily_returns, hdfs_output_daily_returns_path, "Retornos Diários")
        
        # Calcular e salvar as médias dos retornos diários
        average_returns = daily_returns.agg(
            avg("DOLAR_Retorno").alias("Media_DOLAR_Retorno"),
            avg("S&P500_Retorno").alias("Media_SP500_Retorno")
        )
        save_to_hdfs(average_returns, hdfs_output_average_daily_return_path, "Médias dos Retornos Diários")

    except Exception as e:
        print(f"Erro inesperado durante a execução do job Spark: {e}")
        sys.exit(-1)

    finally:
        # Encerrar a sessão Spark
        spark.stop()

if __name__ == "__main__":
    # Verificar os argumentos de entrada
    if len(sys.argv) != 5:
        print("Uso: spark_job.py <initial_date> <final_date> <job_id> <dataset_path>")
        print("Exemplo: spark_job.py 2024-09-15 2024-09-20 123e4567-e89b-12d3-a456-426614174000 /input/market_data.csv")
        sys.exit(-1)

    initial_date = sys.argv[1]
    final_date = sys.argv[2]
    job_id = sys.argv[3]
    dataset_path = sys.argv[4]

    # Executar o job principal
    main(initial_date, final_date, job_id, dataset_path)
