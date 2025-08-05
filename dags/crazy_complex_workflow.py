from datetime import datetime, timedelta
from airflow import DAG
from airflow.decorators import task
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup
import random

# Default args
default_args = {
    'owner': 'chaos-engineer',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 0,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    'crazy_complex_workflow',
    default_args=default_args,
    description='The most visually complex DAG with maximum dependencies and chaos!',
    schedule=None,            # Airflow 3 uses `schedule` instead of schedule_interval
    catchup=False,
    tags=['demo', 'complex', 'chaos', 'visual'],
)

# Start nodes
start = EmptyOperator(task_id='start', dag=dag)
init_phase_1 = EmptyOperator(task_id='initialize_phase_1', dag=dag)
init_phase_2 = EmptyOperator(task_id='initialize_phase_2', dag=dag)

def choose_path(**context):
    choice = random.choice(['path_alpha', 'path_beta', 'path_gamma'])
    print(f"Choosing path: {choice}")
    return choice

branching_decision = BranchPythonOperator(
    task_id='branching_decision',
    python_callable=choose_path,
    dag=dag,
)

# Path Alpha
path_alpha = EmptyOperator(task_id='path_alpha', dag=dag)
alpha_process_1 = BashOperator(task_id='alpha_process_1', bash_command='echo "Alpha 1"', dag=dag)
alpha_process_2 = BashOperator(task_id='alpha_process_2', bash_command='echo "Alpha 2"', dag=dag)
alpha_process_3 = BashOperator(task_id='alpha_process_3', bash_command='echo "Alpha 3"', dag=dag)

# Path Beta
path_beta = EmptyOperator(task_id='path_beta', dag=dag)
beta_process_1 = BashOperator(task_id='beta_process_1', bash_command='echo "Beta 1"', dag=dag)
beta_process_2 = BashOperator(task_id='beta_process_2', bash_command='echo "Beta 2"', dag=dag)

# Path Gamma
path_gamma = EmptyOperator(task_id='path_gamma', dag=dag)
gamma_process_1 = BashOperator(task_id='gamma_process_1', bash_command='echo "Gamma 1"', dag=dag)
gamma_process_2 = BashOperator(task_id='gamma_process_2', bash_command='echo "Gamma 2"', dag=dag)
gamma_process_3 = BashOperator(task_id='gamma_process_3', bash_command='echo "Gamma 3"', dag=dag)
gamma_process_4 = BashOperator(task_id='gamma_process_4', bash_command='echo "Gamma 4"', dag=dag)

convergence_point = EmptyOperator(
    task_id='convergence_point',
    dag=dag,
    trigger_rule='none_failed_or_skipped',
)

# Data Processing Cluster
with TaskGroup("data_processing_cluster", dag=dag) as processing_group:
    cluster_start = EmptyOperator(task_id='cluster_start')
    processors = [
        BashOperator(task_id=f'processor_{i}', bash_command=f'echo "Processing node {i}"')
        for i in range(5)
    ]
    cluster_merge = EmptyOperator(task_id='cluster_merge')

    cluster_start >> [processors[0], processors[1]]
    processors[0] >> processors[2]
    processors[1] >> [processors[2], processors[3]]
    [processors[2], processors[3]] >> processors[4] >> cluster_merge

# Analytics Web
with TaskGroup("analytics_web", dag=dag) as analytics_group:
    web_start = EmptyOperator(task_id='web_start')
    analytics = [
        BashOperator(task_id=f'analytics_{i}', bash_command=f'echo "Analytics {i}"')
        for i in range(4)
    ]
    web_end = EmptyOperator(task_id='web_end')

    web_start >> [analytics[0], analytics[1]]
    [analytics[0], analytics[1]] >> analytics[2]
    analytics[0] >> analytics[3]
    [analytics[2], analytics[3]] >> web_end

# Chaos nodes
chaos_node_1 = BashOperator(task_id='chaos_node_1', bash_command='echo "Chaos 1"', dag=dag)
chaos_node_2 = BashOperator(task_id='chaos_node_2', bash_command='echo "Chaos 2"', dag=dag)
chaos_node_3 = BashOperator(task_id='chaos_node_3', bash_command='echo "Chaos 3"', dag=dag)
chaos_node_4 = BashOperator(task_id='chaos_node_4', bash_command='echo "Chaos 4"', dag=dag)
chaos_node_5 = BashOperator(task_id='chaos_node_5', bash_command='echo "Chaos 5"', dag=dag)

# Fan-out
fan_out_start = EmptyOperator(task_id='fan_out_start', dag=dag)
fan_tasks = [
    BashOperator(task_id=f'fan_task_{i}', bash_command=f'echo "Fan task {i}"', dag=dag)
    for i in range(6)
]

# Diamond
diamond_top = EmptyOperator(task_id='diamond_top', dag=dag)
diamond_left = BashOperator(task_id='diamond_left', bash_command='echo "Diamond left"', dag=dag)
diamond_right = BashOperator(task_id='diamond_right', bash_command='echo "Diamond right"', dag=dag)
diamond_bottom = EmptyOperator(task_id='diamond_bottom', dag=dag)

# Cross
cross_1 = BashOperator(task_id='cross_1', bash_command='echo "Cross 1"', dag=dag)
cross_2 = BashOperator(task_id='cross_2', bash_command='echo "Cross 2"', dag=dag)
cross_3 = BashOperator(task_id='cross_3', bash_command='echo "Cross 3"', dag=dag)
cross_4 = BashOperator(task_id='cross_4', bash_command='echo "Cross 4"', dag=dag)

# Final converge
pre_final_1 = EmptyOperator(task_id='pre_final_1', dag=dag, trigger_rule='none_failed_or_skipped')
pre_final_2 = EmptyOperator(task_id='pre_final_2', dag=dag, trigger_rule='none_failed_or_skipped')
final_merge  = EmptyOperator(task_id='final_merge',  dag=dag, trigger_rule='none_failed_or_skipped')
end          = EmptyOperator(task_id='end',          dag=dag, trigger_rule='none_failed_or_skipped')

# Wire it all up
start >> [init_phase_1, init_phase_2]
init_phase_1 >> branching_decision
init_phase_2 >> chaos_node_1
branching_decision >> [path_alpha, path_beta, path_gamma]
path_alpha >> alpha_process_1 >> alpha_process_2
alpha_process_1 >> alpha_process_3
[alpha_process_2, alpha_process_3] >> convergence_point
path_beta  >> [beta_process_1, beta_process_2] >> convergence_point
path_gamma >> gamma_process_1 >> [gamma_process_2, gamma_process_3]
gamma_process_2 >> gamma_process_4
gamma_process_3 >> gamma_process_4
gamma_process_4 >> convergence_point

chaos_node_1 >> [chaos_node_2, chaos_node_3]
chaos_node_2 >> chaos_node_4
chaos_node_3 >> [chaos_node_4, chaos_node_5]
convergence_point >> processing_group
chaos_node_4 >> analytics_group
chaos_node_5 >> analytics_group

processing_group >> fan_out_start
fan_out_start >> fan_tasks

analytics_group >> diamond_top
diamond_top >> [diamond_left, diamond_right]
[diamond_left, diamond_right] >> diamond_bottom

fan_tasks[0] >> cross_1
fan_tasks[1] >> cross_2
fan_tasks[2] >> [cross_1, cross_3]
fan_tasks[3] >> [cross_2, cross_4]
fan_tasks[4] >> cross_3
fan_tasks[5] >> cross_4

cross_1 >> pre_final_1
cross_2 >> pre_final_1
cross_3 >> pre_final_2
cross_4 >> pre_final_2

diamond_bottom >> final_merge
[pre_final_1, pre_final_2] >> final_merge
final_merge >> end

# Extra cross-connections
chaos_node_2 >> fan_tasks[2]
alpha_process_2 >> chaos_node_3
beta_process_1 >> diamond_left
gamma_process_2 >> cross_2