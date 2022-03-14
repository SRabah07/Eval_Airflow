echo "Stopping..."
docker-compose down -v

echo "Cleaning 'raw_files' and 'clean_data' directories..."

rm -rf ./raw_files/*
rm -rf ./clean_data/*