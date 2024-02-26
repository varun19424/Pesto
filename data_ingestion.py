import json
import csv
import fastavro
from pymongo import MongoClient
import logging

# MongoDB connection
client = MongoClient('mongodb://localhost:27017/')
db = client['advertising_database']

# Function to ingest ad impressions data from JSON into MongoDB
def ingest_ad_impressions_from_json(json_file):
    with open(json_file, 'r') as file:
        ad_impressions_data = json.load(file)
        ad_impressions_collection = db['ad_impressions']
        ad_impressions_collection.insert_many(ad_impressions_data)

# Function to ingest clicks and conversions data from CSV into MongoDB
# Function to ingest clicks and conversions data from CSV into MongoDB
def ingest_clicks_conversions_from_csv(csv_file):
    try:
        with open(csv_file, 'r') as file:
            reader = csv.DictReader(file)
            clicks_conversions_collection = db['clicks_conversions']
            for row in reader:
                # Skip rows with None keys
                if None in row:
                    print(f"Skipping row with None key: {row}")
                    continue
                # Handle missing or None values
                cleaned_row = {k: v if v is not None else '' for k, v in row.items()}
                clicks_conversions_collection.insert_one(cleaned_row)
        print("Data from CSV file ingested successfully into MongoDB.")
    except Exception as e:
        print("An error occurred while ingesting data from CSV file:", e)


# Function to ingest bid request data from Avro into MongoDB
def ingest_bid_requests_from_avro(avro_file):
    try:
        with open(avro_file, 'rb') as file:
            # Add Avro ingestion logic here
            print("Avro file ingestion logic will be implemented here.")
    except FileNotFoundError:
        print(f"Avro file '{avro_file}' not found.")
    except Exception as e:
        print("An error occurred while ingesting data from Avro file:", e)

# Ingest data from respective sources into MongoDB
ingest_ad_impressions_from_json('D:/assignment/Ass/ad_impressions.json')
ingest_clicks_conversions_from_csv('clicks_conversions.csv')  # Assuming the CSV file is in the same directory as the script
ingest_bid_requests_from_avro('bid_requests.avro')  # Assuming the Avro file is in the same directory as the script


# Function to perform data transformation processes
def data_processing():
    # Data validation, filtering, deduplication
    preprocess_ad_impressions()
    preprocess_clicks()
    preprocess_conversions()

    # Correlation of ad impressions with clicks and conversions
    correlate_ad_impressions_with_clicks_and_conversions()


# Function to preprocess ad impressions data
# Function to preprocess ad impressions data
def preprocess_ad_impressions():
    # Function to preprocess ad impressions data
    def preprocess_ad_impressions():
        ad_impressions_collection = db['ad_impressions']
        # Remove documents with duplicate timestamps
        ad_impressions_collection.aggregate([
            {"$group": {"_id": "$timestamp", "count": {"$sum": 1}}},
            {"$match": {"count": {"$gt": 1}}},
        ]).forEach(lambda doc: ad_impressions_collection.delete_many({"timestamp": doc["_id"]}))

        # Now create the index on timestamp
        ad_impressions_collection.create_index([('timestamp', 1)], unique=True)


# Function to preprocess clicks data
def preprocess_clicks():
    clicks_collection = db['clicks']
    # Data validation, filtering, deduplication logic for clicks data
    # Example: Filter out invalid clicks based on timestamps or user IDs


# Function to preprocess conversions data
def preprocess_conversions():
    conversions_collection = db['conversions']
    # Data validation, filtering, deduplication logic for conversions data
    # Example: Validate conversion types and filter out irrelevant conversions


# Function to correlate ad impressions with clicks and conversions
def correlate_ad_impressions_with_clicks_and_conversions():
    ad_impressions_collection = db['ad_impressions']
    clicks_collection = db['clicks']
    conversions_collection = db['conversions']

    # Iterate over ad impressions and find correlated clicks and conversions
    for impression in ad_impressions_collection.find():
        user_id = impression['user_id']
        timestamp = impression['timestamp']

        # Find clicks and conversions for the same user and timestamp
        clicks = list(clicks_collection.find({'user_id': user_id, 'timestamp': timestamp}))
        conversions = list(conversions_collection.find({'user_id': user_id, 'timestamp': timestamp}))

        # Process correlated data or calculate metrics
        if clicks:
            print(f"User {user_id} clicked on ad at {timestamp}: {clicks}")
        if conversions:
            print(f"User {user_id} converted at {timestamp}: {conversions}")


# Execute data processing
data_processing()

# Function to perform analytical queries using MongoDB aggregation framework
def perform_analytical_queries():
    # Example: Aggregate total number of impressions, clicks, and conversions per campaign
    pipeline = [
        {"$group": {"_id": "$ad_campaign_id",
                    "total_impressions": {"$sum": "$impressions"},
                    "total_clicks": {"$sum": "$clicks"},
                    "total_conversions": {"$sum": "$conversions"}}},
        {"$sort": {"_id": 1}}  # Sort results by campaign ID
    ]
    campaign_stats = list(db['campaign_data'].aggregate(pipeline))
    print("Campaign Performance Statistics:")
    for stats in campaign_stats:
        print(f"Campaign ID: {stats['_id']}, Impressions: {stats['total_impressions']}, Clicks: {stats['total_clicks']}, Conversions: {stats['total_conversions']}")

# Execute analytical queries
perform_analytical_queries()



# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Example function to simulate data processing
def process_data(data):
    try:
        # Process data
        logging.info("Data processing successful.")
    except Exception as e:
        # Log error and raise exception
        logging.error(f"Error processing data: {e}")
        raise

# Main function
def main():
    try:
        # Simulate data processing
        data = {}  # Sample data
        process_data(data)
    except Exception as e:
        # Log unhandled exceptions
        logging.exception(f"Unhandled exception: {e}")

if __name__ == "__main__":
    main()