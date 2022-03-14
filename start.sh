echo "Starting..."

echo "Create needed directories..."

mkdir -p ./clean_data
mkdir -p ./raw_files

echo "Init Airflow container 'airflow-init'..."
docker-compose up airflow-init 

echo "Start Airflow..."
docker-compose up -d

#echo "Copy Python sources into Airflow Dags..."
#cp *.py ./dags

echo "Start done..."