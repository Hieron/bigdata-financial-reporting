import atexit
import glob
import os
import pandas as pd
import plotly.graph_objects as go
import re
import shutil
import smtplib
import subprocess
import uuid
import yfinance as yf
from apscheduler.schedulers.background import BackgroundScheduler
from datetime import datetime, timedelta
from email.message import EmailMessage
from flask import Flask, request, jsonify

app = Flask(__name__)

scheduler = BackgroundScheduler()
scheduler.start()

atexit.register(lambda: scheduler.shutdown())

@app.route('/api/jobs', methods=['GET'])
def list_jobs():
    """
    Retorna a lista de jobs agendados com informações básicas.
    """
    jobs = []
    for job in scheduler.get_jobs():
        jobs.append({
            'id': job.id,
            'name': job.name,
            'next_run_time': str(job.next_run_time),
            'trigger': str(job.trigger)
        })
    return jsonify(jobs)

@app.route('/api/submit', methods=['POST'])
def submit_spark_job():
    try:
        data = request.json
        error_message = validate_input(data)
        
        if error_message:
            return jsonify({'success': False, 'error': error_message}), 400

        script_path = data['script_path']
        initial_date = data['initial_date']
        final_date = data['final_date']
        email = data['email']

        try:
            process_spark_job_and_send_report(script_path, initial_date, final_date, email)
            return jsonify({'success': True, 'message': 'Job Spark realizado com sucesso!'})
        except RuntimeError as re:
            return jsonify({'success': False, 'error': f'Erro ao executar o job Spark: {re}'}), 500
        except Exception as e:
            return jsonify({'success': False, 'error': f'Erro inesperado ao executar o job Spark: {e}'}), 500
    except Exception as e:
        return jsonify({'success': False, 'error': f'Erro inesperado na solicitação: {e}'}), 500

@app.route('/api/schedule', methods=['POST'])
def schedule_spark_job():
    try:
        data = request.json
        error_message = validate_input(data)
        
        if error_message:
            return jsonify({'success': False, 'error': error_message}), 400

        script_path = data['script_path']
        initial_date = data['initial_date']
        final_date = data['final_date']
        email = data['email']

        try:
            schedule_time = datetime.now() + timedelta(minutes=1)
            scheduler.add_job(
                process_spark_job_and_send_report,
                'date',
                run_date=schedule_time,
                args=[script_path, initial_date, final_date, email]
            )
            return jsonify({'success': True, 'message': 'Job Spark agendado com sucesso!'})
        except ValueError as ve:
            return jsonify({'success': False, 'error': f'Erro ao agendar o job: {ve}'}), 400
        except Exception as e:
            return jsonify({'success': False, 'error': f'Erro inesperado ao agendar o job: {e}'}), 500
    except Exception as e:
        return jsonify({'success': False, 'error': f'Erro inesperado na solicitação: {e}'}), 500

def process_spark_job_and_send_report(script_path, initial_date, final_date, email):
    """
    Gerencia todo o fluxo de execução do job Spark, cópia dos arquivos, geração de gráficos e envio de relatório.

    Parâmetros:
        script_path (str): Caminho do script Spark a ser executado.
        initial_date (str): Data inicial para o processamento dos dados.
        final_date (str): Data final para o processamento dos dados.
        email (str): Endereço de e-mail do destinatário do relatório.
    
    Exceções:
        RuntimeError: Lançada em caso de erro durante o processamento do job Spark.
        FileNotFoundError: Lançada se algum dos arquivos CSV esperados não for encontrado.
        Exception: Lançada para qualquer outro erro inesperado.
    """
    job_id = None
    local_output_path = None
    
    try:
        print("Iniciando o processamento do job Spark...")
        
        # Primeiro, baixa o dataset localmente
        tickers = [
            "^GSPC", "BRL=X", 
            # "AAPL", "MSFT"
        ]
        column_mapping = {
            "^GSPC": "S&P500",
            "BRL=X": "DOLAR",
            # "AAPL": "Apple",
            # "MSFT": "Microsoft"
        }
        local_dataset_path = fetch_latest_dataset(tickers, column_mapping=column_mapping, dataset_dir="/tmp/dataset")

        # Em seguida, envia para o HDFS, se não estiver lá
        hdfs_dataset_path = upload_to_hdfs(local_dataset_path, "/input")
        print(hdfs_dataset_path)
        
        job_id = execute_spark_job(script_path, initial_date, final_date, hdfs_dataset_path)
        
        # Definir caminhos dos arquivos de entrada e saída
        hdfs_output_path = f"/output/{job_id}"
        local_output_path = f"/tmp/output/{job_id}"
        local_output_daily_returns_path = os.path.join(local_output_path, 'daily_returns.csv')
        local_output_average_daily_return_path = os.path.join(local_output_path, 'average_daily_return.csv')
        
        # Copiar e organizar arquivos do HDFS
        copy_files_and_delete_from_hdfs(hdfs_output_path, local_output_path)
        move_files_and_remove_subdirectories(local_output_path)

        # Verificar se os arquivos CSV foram gerados
        if not os.path.exists(local_output_daily_returns_path) or not os.path.exists(local_output_average_daily_return_path):
            raise FileNotFoundError("Um ou mais arquivos CSV não foram encontrados após o processamento do job Spark.")
        
        # Carregar dados dos CSVs
        daily_returns_df = pd.read_csv(local_output_daily_returns_path)
        average_daily_return_df = pd.read_csv(local_output_average_daily_return_path)
        
        # Contar registros e calcular retornos médios
        daily_returns_count = daily_returns_df.shape[0]
        
        # Verificação de registros no DataFrame
        if daily_returns_count == 0:
            report_body = f"""
                <body>
                    <h2>Prezado,</h2>
                    <p>Para o período solicitado de <strong>{format_date(initial_date)}</strong> até <strong>{format_date(final_date)}</strong>, 
                    não foram encontrados registros para gerar o relatório de mercado.</p>
                    <p>Atenciosamente,<br>Grupo do Trabalho</p>
                </body>
            """
            send_email(
                subject="Relatório de mercado (Trabalho Big Data) - Sem Registros",
                body=report_body,
                to_email=email
            )
            print("Nenhum registro encontrado no período especificado. E-mail de notificação enviado.")
            return

        # Se há registros, calcular retornos médios
        dolar_average_daily_return = float(average_daily_return_df["Media_DOLAR_Retorno"].iloc[0])
        sp500_average_daily_return = float(average_daily_return_df["Media_SP500_Retorno"].iloc[0])
        
        print(f"Número de registros de retornos diários: {daily_returns_count}.")
        
        # Gerar gráficos dos retornos diários
        dolar_daily_returns_plot_path = save_graph(
            daily_returns_df, "Date", "DOLAR_Retorno", "Dólar - Retornos Diários", "dolar_daily_returns.html", local_output_path
        )
        sp500_daily_returns_plot_path = save_graph(
            daily_returns_df, "Date", "S&P500_Retorno", "S&P500 - Retornos Diários", "sp500_daily_returns.html", local_output_path
        )
        
        print("Gráficos gerados com sucesso.")
        
        # Construir o corpo do e-mail em HTML
        report_body = f"""
            <body>
                <h2>Prezado,</h2>
                <p>Para o período solicitado de <strong>{format_date(initial_date)}</strong> até <strong>{format_date(final_date)}</strong>, segue o relatório de mercado:</p>
                <ul>
                    <li>O ativo <strong>USD/BRL (BRL=X)</strong> teve o retorno médio de <strong>{dolar_average_daily_return:.2f}%</strong>.</li>
                    <li>O ativo <strong>S&P 500 (^GSPC)</strong> teve o retorno médio de <strong>{sp500_average_daily_return:.2f}%</strong>.</li>
                    <li>Total de <strong>{daily_returns_count}</strong> registros encontrados.</li>
                </ul>
                <p>Em anexo se encontram também a performance dos ativos no período selecionado.</p>
                <p>Atenciosamente,<br>Grupo do Trabalho</p>
            </body>
        """
        
        # Enviar o e-mail com o relatório e anexos
        send_email(
            subject="Relatório de mercado (Trabalho Big Data)",
            body=report_body,
            to_email=email,
            attachment_paths=[dolar_daily_returns_plot_path, sp500_daily_returns_plot_path]
        )
        
        print("Relatório enviado com sucesso.")
        print("Processamento completo e relatório enviado com sucesso.")
    
    except RuntimeError as e:
        print(f"Erro durante o processamento do job: {e}")
    except FileNotFoundError as fnf_error:
        print(f"Erro: {fnf_error}")
    except ValueError as ve:
        print(f"Erro nos dados: {ve}")
    except Exception as ex:
        print(f"Erro inesperado durante o processamento do job: {str(ex)}")
    finally:
        # Remover o diretório local do job ao terminar o processo
        if local_output_path and os.path.exists(local_output_path):
            try:
                shutil.rmtree(local_output_path)
                print(f"Diretório local '{local_output_path}' removido com sucesso.")
            except Exception as cleanup_error:
                print(f"Erro ao remover o diretório local '{local_output_path}': {cleanup_error}")

def fetch_latest_dataset(tickers, start_date="2000-01-01", end_date=None, dataset_dir="/tmp/data/dataset", dataset_prefix="market_data", column_mapping=None):
    """
    Verifica se o dataset mais recente existe no diretório especificado. Se não existir, baixa os dados mais recentes e salva como CSV.

    Parâmetros:
        tickers (list): Lista de tickers para buscar dados.
        start_date (str): Data de início no formato 'yyyy-mm-dd'. Padrão é '2000-01-01'.
        end_date (str): Data de término no formato 'yyyy-mm-dd'. Se não especificado, será a data atual.
        dataset_dir (str): Diretório onde o dataset será salvo. Padrão é '/tmp/data/dataset'.
        dataset_prefix (str): Prefixo para o nome do arquivo de saída. Padrão é 'market_data'.
        column_mapping (dict): Dicionário para renomear colunas (ex: {'BRL=X': 'DOLAR'}).
    
    Retorna:
        str: Caminho do arquivo CSV gerado ou existente.
    """
    # Define a data final como a data atual, caso não seja especificada
    if end_date is None:
        end_date = datetime.today().strftime('%Y-%m-%d')
    
    # Define a data do dia para o nome do arquivo
    today_date_str = datetime.today().strftime('%Y-%m-%d')

    # Cria o nome completo do arquivo com a data atual no nome
    dataset_name = f"{dataset_prefix}_{today_date_str}.csv"
    dataset_path = os.path.join(dataset_dir, dataset_name)

    # Verifica se o arquivo já existe
    if os.path.exists(dataset_path):
        print(f"Dataset já existe em: {dataset_path}")
        return dataset_path

    # Baixa os dados de mercado para os tickers especificados
    print(f"Baixando dados de mercado para o período de {start_date} a {end_date}...")
    dados_mercado = yf.download(tickers, start=start_date, end=end_date)["Adj Close"]

    # Preencher valores ausentes com 0
    dados_mercado = dados_mercado.fillna(0)

    # Renomear as colunas se o mapeamento for fornecido
    if column_mapping:
        # Verifica se todas as colunas especificadas no mapeamento estão presentes no DataFrame
        missing_columns = [col for col in column_mapping.keys() if col not in dados_mercado.columns]
        if missing_columns:
            raise KeyError(f"As seguintes colunas para renomeação estão ausentes no DataFrame: {missing_columns}")
        
        # Renomeia as colunas com base no mapeamento fornecido
        dados_mercado = dados_mercado.rename(columns=column_mapping)
        print(f"Colunas renomeadas de acordo com o mapeamento: {column_mapping}")
    else:
        # Renomeia as colunas dos tickers para facilitar o uso
        dados_mercado.columns = [ticker.replace('^', '') for ticker in dados_mercado.columns]

    # Verifica se o diretório de destino existe e cria se necessário
    if not os.path.exists(dataset_dir):
        os.makedirs(dataset_dir)
        print(f"Diretório criado: {dataset_dir}")

    # Salva o DataFrame no caminho especificado
    dados_mercado.to_csv(dataset_path)
    print(f"Dados de mercado salvos em: {dataset_path}")

    return dataset_path

def execute_spark_job(script_path, initial_date, final_date, hdfs_dataset_path):
    """
    Executa o job Spark e retorna o job_id.

    Parâmetros:
        script_path (str): Caminho para o script do Spark a ser executado.
        initial_date (str): Data inicial para o processamento dos dados.
        final_date (str): Data final para o processamento dos dados.

    Retorna:
        str: ID único do job executado.

    Exceções:
        RuntimeError: Lançada em caso de erro ao executar o job Spark.
        FileNotFoundError: Lançada se o script especificado não for encontrado.
    """
    job_id = str(uuid.uuid4())
    print(f"Iniciando job Spark com ID único: {job_id}")

    # Verificar se o script Spark existe
    if not os.path.isfile(script_path):
        error_message = f"O script especificado não foi encontrado: {script_path}"
        print(error_message)
        raise FileNotFoundError(error_message)

    # Comando para executar o job Spark
    command = [
        'spark-submit',
        script_path,
        initial_date,
        final_date,
        job_id,
        hdfs_dataset_path
    ]

    try:
        # Executar o comando e capturar a saída
        print(f"Executando comando: {' '.join(command)}")
        result = subprocess.run(command, capture_output=True, text=True, check=True)
        
        # Exibir a saída padrão e de erro, se houver
        print("Saída do job Spark:")
        if result.stdout:
            print(result.stdout)
        if result.stderr:
            print(f"Erros do job Spark (se houver):\n{result.stderr}")
        
        return job_id

    except subprocess.CalledProcessError as e:
        # Erro durante a execução do job Spark
        error_message = (
            f"Erro ao executar o job Spark com ID {job_id}.\n"
            f"Comando: {' '.join(command)}\n"
            f"Código de retorno: {e.returncode}\n"
            f"Erro: {e.stderr}"
        )
        print(error_message)
        raise RuntimeError(error_message)

    except Exception as ex:
        # Outro erro inesperado
        error_message = f"Erro inesperado ao executar o job Spark: {str(ex)}"
        print(error_message)
        raise RuntimeError(error_message)

def copy_files_and_delete_from_hdfs(hdfs_output_path, local_output_path):
    """
    Copia arquivos do HDFS para o sistema de arquivos local e remove a pasta do HDFS após a cópia.

    Parâmetros:
        hdfs_output_path (str): Caminho da pasta no HDFS que será copiada e excluída.
        local_output_path (str): Caminho do diretório local onde os arquivos serão copiados.

    Retorna:
        str: Caminho completo do diretório local onde os arquivos foram copiados.

    Exceções:
        FileNotFoundError: Se a pasta copiada não for encontrada no sistema local após a cópia.
        RuntimeError: Se ocorrer um erro ao copiar ou excluir a pasta do HDFS.
    """
    # Adiciona o esquema HDFS ao caminho de origem
    hdfs_full_path = f"hdfs://coordinator:9000{hdfs_output_path}"
    
    def copy_files():
        """Copia arquivos do HDFS para o diretório local."""
        # Verifica se o diretório local já existe e cria se necessário
        if not os.path.exists(local_output_path):
            os.makedirs(local_output_path)
        
        # Comando para copiar os arquivos do HDFS para o diretório local
        copy_command = ['hdfs', 'dfs', '-get', f"{hdfs_full_path}/*", local_output_path]
        try:
            print(f"Copiando arquivos do HDFS para {local_output_path}...")
            subprocess.run(copy_command, check=True)
            
            # Verifica se o diretório local existe após a cópia
            if not os.path.exists(local_output_path):
                raise FileNotFoundError(f"Diretório não encontrado em {local_output_path}. Verifique se a cópia foi bem-sucedida.")
            
            print(f"Arquivos copiados com sucesso do HDFS para {local_output_path}")
        
        except subprocess.CalledProcessError as e:
            error_message = f"Erro ao copiar arquivos do HDFS: {e.stderr}"
            print(error_message)
            raise RuntimeError(error_message)
    
    def delete_hdfs_directory():
        """Remove a pasta do HDFS após a cópia."""
        delete_command = ['hdfs', 'dfs', '-rm', '-r', hdfs_full_path]
        try:
            print(f"Removendo a pasta {hdfs_output_path} do HDFS...")
            subprocess.run(delete_command, check=True)
            print(f"Pasta {hdfs_output_path} excluída com sucesso do HDFS")
        
        except subprocess.CalledProcessError as e:
            error_message = f"Erro ao excluir pasta do HDFS: {e.stderr}"
            print(error_message)
            raise RuntimeError(error_message)
    
    try:
        # Copiar arquivos do HDFS para o sistema local
        copy_files()
        
        # Remover a pasta do HDFS após a cópia bem-sucedida
        delete_hdfs_directory()
        
        return local_output_path

    except FileNotFoundError as fnf_error:
        print(fnf_error)
        raise fnf_error
    except Exception as ex:
        error_message = f"Erro inesperado ao copiar e excluir arquivos do HDFS: {str(ex)}"
        print(error_message)
        raise RuntimeError(error_message)

def move_files_and_remove_subdirectories(local_output_path):
    """
    Renomeia e move arquivos com padrão 'part*.csv' dos subdiretórios 'daily_returns' e 'average_daily_return'
    para o diretório especificado com novos nomes e remove os subdiretórios após a operação.

    Parâmetros:
        local_output_path (str): Caminho para o diretório local onde os arquivos foram copiados do HDFS.

    Retorna:
        List[str]: Lista com os caminhos completos dos arquivos renomeados.

    Exceções:
        FileNotFoundError: Se nenhum arquivo 'part*.csv' for encontrado nos subdiretórios.
        RuntimeError: Se houver problemas ao renomear ou mover os arquivos.
    """
    try:
        renamed_files = []

        # Verificar se o diretório local de saída existe e é gravável
        if not os.path.exists(local_output_path):
            raise FileNotFoundError(f"Diretório de saída não encontrado: {local_output_path}")
        
        if not os.access(local_output_path, os.W_OK):
            raise PermissionError(f"Permissão de gravação negada para o diretório: {local_output_path}")

        # Define os subdiretórios e os novos nomes para os arquivos
        subdir_file_mapping = {
            "daily_returns": "daily_returns.csv",
            "average_daily_return": "average_daily_return.csv"
        }

        for subdir, new_file_name in subdir_file_mapping.items():
            subdir_path = os.path.join(local_output_path, subdir)
            
            # Verificar se o subdiretório existe
            if not os.path.exists(subdir_path):
                print(f"Subdiretório não encontrado: {subdir_path}. Ignorando.")
                continue
            
            # Buscar o arquivo 'part*.csv' dentro do subdiretório
            part_files = glob.glob(os.path.join(subdir_path, "part*.csv"))
            
            # Verificar se não há arquivos part*.csv no subdiretório
            if not part_files:
                print(f"Nenhum arquivo 'part*.csv' encontrado em {subdir_path}. Ignorando.")
                continue
            
            # Verificar se há mais de um arquivo 'part*.csv' no subdiretório
            if len(part_files) > 1:
                raise RuntimeError(f"Mais de um arquivo 'part*.csv' encontrado em {subdir_path}: {part_files}")
            
            # Renomear e mover o arquivo para o diretório local de saída
            part_file = part_files[0]
            new_file_path = os.path.join(local_output_path, new_file_name)
            shutil.move(part_file, new_file_path)
            print(f"Arquivo renomeado de {part_file} para {new_file_path}")
            renamed_files.append(new_file_path)

            # Remover o subdiretório após mover o arquivo
            shutil.rmtree(subdir_path)
            print(f"Subdiretório {subdir_path} removido com sucesso.")

        return renamed_files

    except (FileNotFoundError, PermissionError) as fnf_error:
        # Erros de permissão ou diretório não encontrado
        print(f"Erro: {fnf_error}")
        raise
    except Exception as ex:
        # Qualquer outro erro durante o processamento
        error_message = f"Erro ao mover e renomear arquivos: {str(ex)}"
        print(error_message)
        raise RuntimeError(error_message)

def save_graph(df, x_col, y_col, title, filename, dataset_dir='.', line_mode='lines', line_color='blue', line_width=2):
    """
    Gera e salva um gráfico de linha utilizando o Plotly com base em um DataFrame e salva como arquivo HTML.

    Parâmetros:
        df (pd.DataFrame): DataFrame com os dados a serem plotados.
        x_col (str): Nome da coluna que será usada como eixo X.
        y_col (str): Nome da coluna que será usada como eixo Y.
        title (str): Título do gráfico.
        filename (str): Nome do arquivo HTML para salvar o gráfico.
        dataset_dir (str): Diretório onde o arquivo será salvo. Padrão é o diretório atual.
        line_mode (str): Modo de linha para o gráfico (padrão é 'lines'). Ex.: 'lines', 'markers', 'lines+markers'.
        line_color (str): Cor da linha do gráfico (padrão é 'blue').
        line_width (int): Largura da linha do gráfico (padrão é 2).

    Retorna:
        str: Caminho completo do arquivo HTML salvo.

    Exceções:
        KeyError: Se as colunas especificadas não existirem no DataFrame.
        ValueError: Se o DataFrame estiver vazio ou se as colunas especificadas não forem numéricas ou de data.
    """
    # Verificar se as colunas x_col e y_col estão presentes no DataFrame
    if x_col not in df.columns:
        raise KeyError(f"A coluna '{x_col}' não está presente no DataFrame. Colunas disponíveis: {df.columns.tolist()}")
    if y_col not in df.columns:
        raise KeyError(f"A coluna '{y_col}' não está presente no DataFrame. Colunas disponíveis: {df.columns.tolist()}")

    # Verificar se o DataFrame não está vazio
    if df.empty:
        raise ValueError("O DataFrame está vazio. Verifique se os dados foram carregados corretamente.")

    # Verificar se as colunas x_col e y_col são numéricas ou de data
    # if not pd.api.types.is_numeric_dtype(df[x_col]) and not pd.api.types.is_datetime64_any_dtype(df[x_col]):
    #     raise ValueError(f"A coluna '{x_col}' deve ser numérica ou de data.")
    # if not pd.api.types.is_numeric_dtype(df[y_col]):
    #     raise ValueError(f"A coluna '{y_col}' deve ser numérica.")

    # Criar a figura do gráfico
    fig = go.Figure()

    # Adicionar a linha ao gráfico com os parâmetros personalizáveis
    fig.add_trace(go.Scatter(
        x=df[x_col], 
        y=df[y_col], 
        mode=line_mode, 
        name=y_col,
        line=dict(color=line_color, width=line_width)
    ))
    
    # Atualizar o layout do gráfico
    fig.update_layout(
        title=title,
        xaxis_title=x_col,
        yaxis_title=y_col,
        xaxis_tickangle=-45
    )
    
    # Verificar se o diretório de destino existe e criá-lo se necessário
    if not os.path.exists(dataset_dir):
        try:
            os.makedirs(dataset_dir)
            print(f"Diretório criado: {dataset_dir}")
        except OSError as e:
            raise RuntimeError(f"Erro ao criar o diretório {dataset_dir}: {e}")
    
    # Caminho completo para o arquivo de saída
    output_path = os.path.join(dataset_dir, filename)
    
    try:
        # Salvar o gráfico como HTML
        fig.write_html(output_path)
        print(f"Gráfico salvo em: {output_path}")
    except Exception as e:
        raise RuntimeError(f"Erro ao salvar o gráfico como HTML: {e}")
    
    return output_path

def send_email(subject, body, to_email, attachment_paths=None):
    """
    Função para enviar um relatório por e-mail com múltiplos anexos de diferentes formatos, incluindo imagens.

    Parâmetros:
        subject (str): Assunto do e-mail.
        body (str): Corpo do e-mail. Pode ser HTML.
        to_email (str): Endereço de e-mail do destinatário.
        attachment_paths (list, opcional): Lista de caminhos para anexos. Cada caminho deve apontar para um arquivo local que será anexado ao e-mail.

    Variáveis de ambiente:
        EMAIL_USER: Endereço de e-mail do remetente (usuário SMTP).
        EMAIL_PASS: Senha do e-mail do remetente (senha SMTP).

    Exceções:
        FileNotFoundError: Lançada se um dos arquivos de anexo não for encontrado.
        RuntimeError: Lançada em caso de erro ao enviar o e-mail ou ao anexar arquivos.

    """
    
    #Obter e-mail e senha das variáveis de ambiente
    smtp_server = os.getenv('CONTROLLER_SENDER_SERVER')
    smtp_port = os.getenv('CONTROLLER_SENDER_PORT')
    from_email = os.getenv('CONTROLLER_SENDER_EMAIL')
    password = os.getenv('CONTROLLER_SENDER_PASSWORD')

    if not from_email or not password:
        raise RuntimeError("As variáveis de ambiente 'EMAIL_USER' e 'EMAIL_PASS' precisam estar definidas.")

    msg = EmailMessage()
    msg['Subject'] = subject
    msg['From'] = from_email
    msg['To'] = to_email
    
    # Definir o conteúdo do e-mail como HTML
    msg.add_alternative(body, subtype='html')
    
    if attachment_paths:
        for attachment_path in attachment_paths:
            try:
                with open(attachment_path, 'rb') as f:
                    file_data = f.read()
                    file_name = os.path.basename(attachment_path)
                    
                    # Detecta o tipo de anexo baseado na extensão do arquivo
                    if file_name.endswith('.pdf'):
                        maintype, subtype = 'application', 'pdf'
                    elif file_name.endswith('.html'):
                        maintype, subtype = 'text', 'html'
                    elif file_name.endswith('.csv'):
                        maintype, subtype = 'text', 'csv'
                    elif file_name.endswith('.txt'):
                        maintype, subtype = 'text', 'plain'
                    elif file_name.endswith(('.png', '.jpg', '.jpeg')):
                        maintype, subtype = 'image', file_name.split('.')[-1]  # Define 'image/png' ou 'image/jpeg'
                    else:
                        # Default para arquivos desconhecidos
                        maintype, subtype = 'application', 'octet-stream'
                    
                    msg.add_attachment(file_data, maintype=maintype, subtype=subtype, filename=file_name)
            except FileNotFoundError:
                print(f"Arquivo {attachment_path} não encontrado.")
                continue  # Pula esse anexo e continua com os outros
    
    try:
        with smtplib.SMTP_SSL(smtp_server, smtp_port) as server:
            server.login(from_email, password)
            server.send_message(msg)
            print(f"Relatório enviado para {to_email}")
    except smtplib.SMTPException as e:
        error_message = f"Erro ao enviar o e-mail: {e}"
        print(error_message)
        raise RuntimeError(error_message)
    except Exception as ex:
        error_message = f"Erro inesperado ao enviar o e-mail: {str(ex)}"
        print(error_message)
        raise RuntimeError(error_message)

def format_date(value):
    """
    Converte uma data no formato 'yyyy-mm-dd' para o formato 'dd/mm/yyyy'.

    Parâmetros:
        value (str): Data no formato 'yyyy-mm-dd'.

    Retorna:
        str: Data formatada no formato 'dd/mm/yyyy'.

    Exceções:
        ValueError: Lançada se a data fornecida não estiver no formato esperado.
    """
    try:
        # Converter a string para um objeto datetime
        date_obj = datetime.strptime(value, '%Y-%m-%d')
        # Retornar a data formatada como 'dd/mm/yyyy'
        return date_obj.strftime('%d/%m/%Y')
    except ValueError:
        raise ValueError(f"Formato de data inválido: '{value}'. Esperado 'yyyy-mm-dd'.")

def validate_input(data):
    """
    Valida os dados de entrada e retorna uma mensagem de erro se algo estiver faltando ou incorreto.

    Parâmetros:
        data (dict): Dicionário contendo os dados de entrada.

    Retorna:
        str: Mensagem de erro se houver problemas nos dados de entrada, ou None se todos os dados forem válidos.
    """
    required_fields = {
        'script_path': 'O caminho do script é obrigatório e está ausente ou vazio.',
        'initial_date': 'A data inicial é obrigatória e está ausente ou vazia.',
        'final_date': 'A data final é obrigatória e está ausente ou vazia.',
        'email': 'O e-mail é obrigatório e está ausente ou vazio.'
    }

    # Verificar se todos os campos obrigatórios estão presentes e não estão vazios
    for field, error_message in required_fields.items():
        if field not in data or not data[field]:
            return error_message

    # Validar formato de e-mail
    email = data['email']
    email_pattern = r'^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$'
    if not re.match(email_pattern, email):
        return 'O e-mail fornecido está em um formato inválido.'

    # Validar formato de data
    date_format = '%Y-%m-%d'
    for date_field in ['initial_date', 'final_date']:
        try:
            datetime.strptime(data[date_field], date_format)
        except ValueError:
            return f'A data "{data[date_field]}" está em um formato inválido. Use o formato "yyyy-mm-dd".'

    return None

def check_hdfs_file_exists(hdfs_path):
    """
    Verifica se um arquivo ou diretório já existe no HDFS.

    Parâmetros:
        hdfs_path (str): Caminho completo no HDFS para verificar.
    
    Retorna:
        bool: True se o arquivo/diretório existir, False caso contrário.
    """
    check_command = ['hdfs', 'dfs', '-test', '-e', hdfs_path]
    try:
        subprocess.run(check_command, check=True)
        print(f"O arquivo ou diretório {hdfs_path} já existe no HDFS.")
        return True
    except subprocess.CalledProcessError:
        # Se o comando retorna um erro, significa que o arquivo não existe
        print(f"O arquivo ou diretório {hdfs_path} não existe no HDFS.")
        return False
    
def create_hdfs_directory(hdfs_path):
    """
    Cria um diretório no HDFS, incluindo todos os diretórios pai, se necessário.

    Parâmetros:
        hdfs_path (str): Caminho completo do diretório a ser criado no HDFS.
    
    Exceções:
        RuntimeError: Se ocorrer um erro ao criar o diretório no HDFS.
    """
    mkdir_command = ['hdfs', 'dfs', '-mkdir', '-p', hdfs_path]
    try:
        subprocess.run(mkdir_command, check=True)
        print(f"Diretório {hdfs_path} criado com sucesso no HDFS.")
    except subprocess.CalledProcessError as e:
        error_message = f"Erro ao criar diretório no HDFS: {e.stderr}"
        print(error_message)
        raise RuntimeError(error_message)

def upload_to_hdfs(local_dataset_path, hdfs_input_path):
    """
    Envia um arquivo local para um diretório especificado no HDFS, criando o caminho completo se necessário.

    Parâmetros:
        local_dataset_path (str): Caminho completo do arquivo local que será enviado ao HDFS.
        hdfs_input_path (str): Diretório de destino no HDFS onde o arquivo será enviado.
    
    Retorna:
        str: Caminho completo do arquivo no HDFS.
    
    Exceções:
        FileNotFoundError: Se o arquivo local não for encontrado.
        RuntimeError: Se ocorrer um erro ao enviar o arquivo para o HDFS.
    """
    if not os.path.exists(local_dataset_path):
        raise FileNotFoundError(f"Arquivo local não encontrado: {local_dataset_path}")

    # Define o caminho completo do arquivo no HDFS
    hdfs_dataset_path = os.path.join(hdfs_input_path, os.path.basename(local_dataset_path))
    
    # Verificar se o arquivo já existe no HDFS
    if check_hdfs_file_exists(hdfs_dataset_path):
        print(f"O arquivo {hdfs_dataset_path} já existe no HDFS. Nenhuma ação necessária.")
        return hdfs_dataset_path

    # Verificar se o diretório pai no HDFS existe, e criar se necessário
    hdfs_directory = os.path.dirname(hdfs_dataset_path)
    if not check_hdfs_file_exists(hdfs_directory):
        print(f"O diretório {hdfs_directory} não existe no HDFS. Criando...")
        create_hdfs_directory(hdfs_directory)
    
    # Comando para enviar o arquivo para o HDFS
    hdfs_put_command = ['hdfs', 'dfs', '-put', '-f', local_dataset_path, hdfs_dataset_path]
    
    try:
        # Executa o comando para enviar o arquivo ao HDFS
        print(f"Enviando {local_dataset_path} para o HDFS em {hdfs_dataset_path}...")
        subprocess.run(hdfs_put_command, check=True)
        print(f"Arquivo enviado com sucesso para o HDFS: {hdfs_dataset_path}")
    except subprocess.CalledProcessError as e:
        error_message = f"Erro ao enviar o arquivo para o HDFS: {e.stderr}"
        print(error_message)
        raise RuntimeError(error_message)
    
    return hdfs_dataset_path

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=6000)
