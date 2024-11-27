import pandas as pd
import psycopg2
import requests 
import json
from kafka import KafkaProducer
from kafka import KafkaConsumer

#defining kafka producer
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda m: json.dumps(m).encode('ascii')
)
#Defining function that consumes information, and stores it in Kafka server
def consume_from_kafka():
    try:
        consumer = KafkaConsumer(
            'retail_sales',
            bootstrap_servers=['localhost:9092'],
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            group_id='my-group',
            value_deserializer=lambda m: json.loads(m.decode('ascii'))
        )
        #UNCOMMENT THIS LOOP IF YOU WANT TO PRINT THE MESSAGE
        #for message in consumer:
            #print(f"Received message: {message.value}")
            # Insert the received message into PostgreSQL here
    except Exception as e:
        print("An error occurred while consuming messages:", e)


#Function to extract and transform data from CSV, API, and Database
def extract_and_transform():
    transformed_customers = []
    transformed_products = []
    
    #Extracting from CSV
    print("Extracting data...")
    df = pd.read_csv('Retail Sales Dataset_exported.csv')
    df = df.drop(columns=[df.columns[-1]]) #Dropping useless column
    try:
        for index, row in df.iterrows():
            product_record = {
                'Transaction ID': row['Transaction ID'],
                'Customer ID': row['Customer ID'],
                'Product Category': row['Product Category'],
                'Product_id': row['Product_id'],
            }
            transformed_products.append(product_record)
            producer.send('retail_sales', product_record)  
        producer.flush()  
        print("Product data successfully extracted from CSV file!")
    except Exception as e:
        print("Error extracting product data from CSV:", e)
        
    #Extracting from API
    api_url = 'https://dummyjson.com/users'
    try:
        response = requests.get(api_url)
        if response.status_code == 200:
            customer_data = response.json().get('users', [])
            print("Fetched customer data from API.")

            # Prepare the customer data for loading into PostgreSQL
            for user in customer_data:
                customer_record = {
                    'CustomerID': user['id'],
                    'FirstName': user['firstName'],
                    'LastName': user['lastName'],
                    'Email': user['email'],
                    'City': user.get('address', {}).get('city', 'Unknown')
                }
                transformed_customers.append(customer_record)
                # Send to Kafka
                producer.send('retail_sales', customer_record)
            
            producer.flush()  # Ensure all messages are sent
            print("Customer data successfully extracted and transformed from API.")
        else:
            print("Error fetching data from API:", response.status_code, response.text)
    except requests.exceptions.JSONDecodeError as e:
        print("JSON decode error:", e)
        
    # Extract from PostgreSQL
    try:
        conn = psycopg2.connect( #Insert your own information from PostgreSQL
            host='',
            port='',
            database='',
            user='',
            password=''
        )
        cursor = conn.cursor()

        # Fetch all records from the Products table
        cursor.execute("SELECT * FROM products_details;") 
        records = cursor.fetchall()
        for record in records:
            producer.send('retail_sales', dict(zip([desc[0] for desc in cursor.description], record)))  # Send each record as a message
        producer.flush()  
        cursor.close()
        conn.close()
        print("Data successfully extracted from PostgreSQL!")
    except Exception as e:
        print("Error extracting data from PostgreSQL:", e)
        
    return transformed_customers, transformed_products

#Loading customers and transactions data into PostgreSQL (not loading products as it is already in my database )
def load(customers_data, products_data):
    conn = psycopg2.connect(host='',
            port='',
            database='',
            user='',
            password='')
    cursor = conn.cursor()
    
    # Load customer data
    for record in customers_data:
        try:
            cursor.execute('''
                INSERT INTO Customers (CustomerID, FirstName, LastName, Email, City)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (CustomerID) DO NOTHING;
            ''', (record['CustomerID'], record['FirstName'], record['LastName'], record['Email'], record['City']))
        except Exception as e:
            print("Error loading customer record:", e)
            
    # Load transactions data
    conn.commit()
    cursor.close()
    conn.close()
    print("Data successfully loaded into PostgreSQL.")

#Extract, transform, and load the data, and consume it with kafka
if __name__ == "__main__":
    customers_data, products_data = extract_and_transform()  
    load(customers_data, products_data)         
    consume_from_kafka()
    
