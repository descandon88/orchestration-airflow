from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

# Configuración del DAG
default_args = {
    'owner': 'data-team',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'etl_ecommerce',
    default_args=default_args,
    description='Pipeline ETL de datos de e-commerce',
    schedule_interval='@daily',  # Corre todos los días a medianoche
    start_date=datetime(2023, 1, 1),
    catchup=False,  # No ejecutar para fechas pasadas
    tags=['etl', 'ecommerce'],
)

DATA_PATH = '/opt/airflow/data'


# -------------------
# EXTRACT
# -------------------
def extract(**context):
    import os
    
    print("📥 Extracting data...")

    files = {
        'orders': f"{DATA_PATH}/ecommerce_orders.csv",
        'order_items': f"{DATA_PATH}/ecommerce_order_items.csv",
       # 'products': f"{DATA_PATH}/ecommerce_products.csv",
        'customers': f"{DATA_PATH}/ecommerce_customers.csv",
       # 'categories': f"{DATA_PATH}/ecommerce_categories.csv",
       # 'brands': f"{DATA_PATH}/ecommerce_brands.csv",
       # 'inventory': f"{DATA_PATH}/ecommerce_inventory.csv",
       # 'suppliers': f"{DATA_PATH}/ecommerce_suppliers.csv",
       # 'promotions': f"{DATA_PATH}/ecommerce_promotions.csv",
       # 'reviews': f"{DATA_PATH}/ecommerce_reviews.csv",
       # 'warehouses': f"{DATA_PATH}/ecommerce_warehouses.csv",
    }

    # Validar que existan
    for name, path in files.items():
        if not os.path.exists(path):
            raise FileNotFoundError(f"{path} not found")

    context['ti'].xcom_push(key='files', value=files)

    print("✅ All files validated")

extract_task = PythonOperator(
    task_id='extract',
    python_callable=extract,
    dag=dag,
)


# -------------------
# TRANSFORM
# -------------------
def transform(**context):
    import pandas as pd

    print("🔄 Transforming data...")

    files = context['ti'].xcom_pull(key='files', task_ids='extract')

    # Load data
    orders = pd.read_csv(files['orders'])
    items = pd.read_csv(files['order_items'])
    # products = pd.read_csv(files['products'])
    customers = pd.read_csv(files['customers'])
    # categories = pd.read_csv(files['categories'])
    # brands = pd.read_csv(files['brands'])

    # --- Joins principales ---
    df = items.merge(orders, on='order_id', how='left') \
            .merge(customers, on='customer_id', how='left')

        #      .merge(products, on='product_id', how='left') \
         #     .merge(categories, on='category_id', how='left') \
          #    .merge(brands, on='brand_id', how='left') \

    # --- Limpieza ---
    df['order_date'] = pd.to_datetime(df['order_date'], errors='coerce')
    df['order_month'] = df['order_date'].dt.to_period('M').astype(str)

    # --- Métricas ---
    metrics = {
        'total_orders': int(df['order_id'].nunique()),
        'total_revenue': float(df['total_amount'].sum()),
        'avg_order_value': float(df.groupby('order_id')['total_amount'].sum().mean()),
        'total_customers': int(df['customer_id'].nunique()),
    }

    # --- Output ---
    output_path = '/opt/airflow/output/fact_orders.parquet'
    df.to_parquet(output_path, index=False)

    context['ti'].xcom_push(key='metrics', value=metrics)
    context['ti'].xcom_push(key='output_path', value=output_path)

    print("✅ Transformation complete")

transform_task = PythonOperator(
    task_id='transform',
    python_callable=transform,
    dag=dag,
)

# -------------------
# LOAD
# -------------------
def load(**context):
    import json

    print("💾 Loading data...")

    metrics = context['ti'].xcom_pull(key='metrics', task_ids='transform')
    output_path = context['ti'].xcom_pull(key='output_path', task_ids='transform')

    summary = {
        'execution_date': str(context['execution_date']),
        'metrics': metrics,
        'output_file': output_path,
    }

    with open('/opt/airflow/output/summary.json', 'w') as f:
        json.dump(summary, f, indent=2)

    print("✅ Load complete")

load_task = PythonOperator(
    task_id='load',
    python_callable=load,
    dag=dag,
)



# Dependencies 
# extract_task.set_downstream(transform_task)
# transform_task.set_downstream(load_task)

extract_task >> transform_task >> load_task