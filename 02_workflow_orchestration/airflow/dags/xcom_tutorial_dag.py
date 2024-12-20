from random import randint, uniform
from datetime import datetime


from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator

default_args = {
    "start_date": datetime.now(),
}

def _training_model(ti):
    accuracy = uniform(0.1, 10.0)
    print(f"Model's accuracy: {accuracy}")
    ti.xcom_push(key="model_accuracy", value=accuracy) # the current task's value is being communicated
    
def _choose_best_model(ti):
    print("choose best model")
    accuracies = ti.xcom_pull(key="model accuracy", task_ids=["training_model_A", "training_model_B", "training_model_C"]) # Each of the current task's accuracy is being pulled
    print(accuracies)
    
with DAG("xcom_dag", schedule_interval="@daily", default_args=default_args, catchup=False) as dag:
    
    downloading_data = BashOperator(
        task_id="downloading_data", 
        bash_command="sleep 3", 
        do_xcom_push=False # By default, XCOM push is enabled
    )
    
    training_model_task = [
        PythonOperator(
            task_id=f"training_model_{task}",
            python_callable=_training_model
        ) for task in ["A", "B", "C"]]
    
    choose_model = PythonOperator(
        task_id="choose_model",
        python_callable=_choose_best_model
    )
    
    downloading_data >> training_model_task >> choose_model