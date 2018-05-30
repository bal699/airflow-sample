from datetime import datetime
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators import BashOperator

# função para conversao/transformacao dos dados
def converter_dados():
    pass

#*******************************************
# Criação do objeto que representará a DAG
#*******************************************

dag = DAG(
    'carga_diaria_faturamento', 
    description='Extracao, Transformacao e Carga de dados de faturamento',
    schedule_interval='0 12 * * *',
    start_date=datetime(2017, 3, 20), 
    catchup=False
)

#*******************************************
# Configura as Tarefas do Fluxo de Trabalho
#*******************************************

# (1) capturar dados de um Webservice
capturar_webservice = DummyOperator(
    task_id='tarefa_capturar_webservice', 
    retries=3, 
    dag=dag
)

# (2) converter os dados para um formato estruturado; 
converter_dados = PythonOperator(
    task_id='tarefa_converter_dados', 
    python_callable=converter_dados, 
    dag=dag
)

# (3) transformar os dados em informações úteis para o contexto
agrupar_itens_documentos = BashOperator(
    task_id='tarefa_agrupar_itens_documentos',
    bash_command='echo "Agrupando itens dos documentos"',
    dag=dag
)

# (4) aplicar lógica de negócios
calcular_custo_mercadorias = BashOperator(
    task_id='tarefa_calcular_custo_mercadorias',
    bash_command='echo "Calculando o custo das mercadorias vendidas"',
    dag=dag
)

# (5) validar as regras de negócios
validar_documentos_fiscais = BashOperator(
    task_id='validar_documentos_fiscais',
    bash_command='echo "Validando informacoes dos documentos fiscais"',
    dag=dag
)

# (6) gerar novas informações
gerar_campanhas_comerciais = BashOperator(
    task_id='tarefa_gerar_campanhas_comerciais',
    bash_command='echo "Gerando novas campanhas de vendas"',
    dag=dag
)

# (7) armazenar as informações processadas
gravar_banco_dados = BashOperator(
    task_id='tarefa_gravar_banco_dados',
    bash_command='echo "Gravando informacoes no banco de dados"',
    dag=dag
)

# (8) enviar as informações por e-mail
enviar_email = BashOperator(
    task_id='tarefa_enviar_email',
    bash_command='echo "Enviando emails para os gerentes de loja"',
    dag=dag
)

# (9) disponibilizar as informações para os sistemas de inteligência de negócios (Business Intelligence - BI)
atualizar_bi = BashOperator(
    task_id='tarefa_atualizar_bi',
    bash_command='echo "Atualizando aplicacao de Business Intelligence"',
    dag=dag
)

# (9) disponibilizar as informações para os sistemas de inteligência de negócios (Business Intelligence - BI)
notificar_conclusao = BashOperator(
    task_id='tarefa_notificar_conclusao',
    bash_command='echo "Notificando desenvolvedores"',
    dag=dag
)

#**************************************************************
# Configura a sequencia de execução e dependência das tarefas
#**************************************************************

capturar_webservice >> converter_dados >> agrupar_itens_documentos >> [validar_documentos_fiscais, calcular_custo_mercadorias, gerar_campanhas_comerciais] >> gravar_banco_dados >> [enviar_email, atualizar_bi] >> notificar_conclusao
