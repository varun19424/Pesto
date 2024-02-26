import json
import csv
import fastavro

# Sample JSON data representing ad impressions
ad_impressions_json = '''
[
    {"ad_creative_id": 1, "user_id": 123, "timestamp": "2024-02-26 08:30:00", "website": "example.com"},
    {"ad_creative_id": 2, "user_id": 456, "timestamp": "2024-02-26 09:15:00", "website": "example.net"},
    {"ad_creative_id": 3, "user_id": 789, "timestamp": "2024-02-26 10:00:00", "website": "example.org"}
]
'''

# Function to save ad impressions data to a JSON file
def save_ad_impressions_to_json(ad_impressions_data, filename):
    with open(filename, 'w') as json_file:
        json.dump(ad_impressions_data, json_file, indent=4)

# Save ad impressions data to a JSON file
save_ad_impressions_to_json(ad_impressions_json, 'ad_impressions.json')




# Sample CSV data representing clicks and conversions
clicks_conversions_csv = '''
Timestamp,User ID,Ad Campaign ID,Conversion Type
2024-02-26 08:35:00,123,1,Signup
2024-02-26 09:20:00,456,2,Purchase
2024-02-26 10:05:00,789,3,Signup
'''

# Function to process click and conversion data
def save_clicks_conversions_to_csv(clicks_conversions_data, filename):
    with open(filename, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerows(csv.reader(clicks_conversions_data.splitlines()))

# Save click and conversion data to a CSV file
save_clicks_conversions_to_csv(clicks_conversions_csv, 'clicks_conversions.csv')

