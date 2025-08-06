from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule
import random

@dag(
    dag_id='crazy_complex_workflow',
    default_args={
        'owner': 'chaos-engineer',
        'depends_on_past': False,
        'retries': 0,
        'retry_delay': timedelta(minutes=1),
    },
    description='The most visually complex DAG with maximum dependencies and chaos!',
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['demo', 'complex', 'chaos', 'visual', 'astro'],
    doc_md="""
    # Crazy Complex Workflow
    
    This DAG demonstrates a complex workflow pattern with:
    - Branching logic with random path selection
    - Task groups for organized processing
    - Fan-out and convergence patterns
    - Cross-dependencies between different workflow paths
    
    **Compatible with Astro Runtime 3.0+**
    """,
)
def crazy_complex_workflow():

    # Start nodes
    start = EmptyOperator(task_id='start')
    init_phase_1 = EmptyOperator(task_id='initialize_phase_1')
    init_phase_2 = EmptyOperator(task_id='initialize_phase_2')

    @task.branch
    def choose_path():
        choice = random.choice(['path_alpha', 'path_beta', 'path_gamma'])
        print(f"Choosing path: {choice}")
        return choice

    branching_decision = choose_path()

    # Path Alpha
    path_alpha = EmptyOperator(task_id='path_alpha')
    alpha_process_1 = BashOperator(task_id='alpha_process_1', bash_command='echo "Alpha 1"')
    alpha_process_2 = BashOperator(task_id='alpha_process_2', bash_command='echo "Alpha 2"')
    alpha_process_3 = BashOperator(task_id='alpha_process_3', bash_command='echo "Alpha 3"')

    # Path Beta
    path_beta = EmptyOperator(task_id='path_beta')
    beta_process_1 = BashOperator(task_id='beta_process_1', bash_command='echo "Beta 1"')
    beta_process_2 = BashOperator(task_id='beta_process_2', bash_command='echo "Beta 2"')

    # Path Gamma
    path_gamma = EmptyOperator(task_id='path_gamma')
    gamma_process_1 = BashOperator(task_id='gamma_process_1', bash_command='echo "Gamma 1"')
    gamma_process_2 = BashOperator(task_id='gamma_process_2', bash_command='echo "Gamma 2"')
    gamma_process_3 = BashOperator(task_id='gamma_process_3', bash_command='echo "Gamma 3"')
    gamma_process_4 = BashOperator(task_id='gamma_process_4', bash_command='echo "Gamma 4"')

    convergence_point = EmptyOperator(
        task_id='convergence_point',
        trigger_rule=TriggerRule.NONE_FAILED_OR_SKIPPED,
    )

    # Data Processing Cluster
    with TaskGroup("data_processing_cluster") as processing_group:
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
    with TaskGroup("analytics_web") as analytics_group:
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
    chaos_node_1 = BashOperator(task_id='chaos_node_1', bash_command='echo "Chaos 1"')
    chaos_node_2 = BashOperator(task_id='chaos_node_2', bash_command='echo "Chaos 2"')
    chaos_node_3 = BashOperator(task_id='chaos_node_3', bash_command='echo "Chaos 3"')
    chaos_node_4 = BashOperator(task_id='chaos_node_4', bash_command='echo "Chaos 4"')
    chaos_node_5 = BashOperator(task_id='chaos_node_5', bash_command='echo "Chaos 5"')

    # Fan-out
    fan_out_start = EmptyOperator(task_id='fan_out_start')
    fan_tasks = [
        BashOperator(task_id=f'fan_task_{i}', bash_command=f'echo "Fan task {i}"')
        for i in range(6)
    ]

    # Diamond
    diamond_top = EmptyOperator(task_id='diamond_top')
    diamond_left = BashOperator(task_id='diamond_left', bash_command='echo "Diamond left"')
    diamond_right = BashOperator(task_id='diamond_right', bash_command='echo "Diamond right"')
    diamond_bottom = EmptyOperator(task_id='diamond_bottom')

    # Cross
    cross_1 = BashOperator(task_id='cross_1', bash_command='echo "Cross 1"')
    cross_2 = BashOperator(task_id='cross_2', bash_command='echo "Cross 2"')
    cross_3 = BashOperator(task_id='cross_3', bash_command='echo "Cross 3"')
    cross_4 = BashOperator(task_id='cross_4', bash_command='echo "Cross 4"')

    # Final converge
    pre_final_1 = EmptyOperator(task_id='pre_final_1', trigger_rule=TriggerRule.NONE_FAILED_OR_SKIPPED)
    pre_final_2 = EmptyOperator(task_id='pre_final_2', trigger_rule=TriggerRule.NONE_FAILED_OR_SKIPPED)
    final_merge = EmptyOperator(task_id='final_merge', trigger_rule=TriggerRule.NONE_FAILED_OR_SKIPPED)
    end = EmptyOperator(task_id='end', trigger_rule=TriggerRule.NONE_FAILED_OR_SKIPPED)

    # Wire it all up
    start >> [init_phase_1, init_phase_2]
    init_phase_1 >> branching_decision
    init_phase_2 >> chaos_node_1
    branching_decision >> [path_alpha, path_beta, path_gamma]
    path_alpha >> alpha_process_1 >> alpha_process_2
    alpha_process_1 >> alpha_process_3
    [alpha_process_2, alpha_process_3] >> convergence_point
    path_beta >> [beta_process_1, beta_process_2] >> convergence_point
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


crazy_complex_workflow()