# Transform data
#Step 1: Import packages
import os
import glob
import pandas as pd
import sqlalchemy


#Step 2: Define input and output paths
input_path = '/home/ubuntu/venprabhu/rawdata/'
output_path = '/home/ubuntu/venprabhu/processed/'  # Output directory for processed files

#Step 3: Define function to transform data
def convert_units(df):
    df['height'] = df['height'] * 0.0254  # Convert height to meters
    df['weight'] = df['weight'] * 0.453592  # Convert weight to kilograms
    return df  # Ensure the modified DataFrame is returned

#Step 4: Read files and combine dataframe
user = []

# Read CSV files
#csv_files = glob.glob(os.path.join(input_path, '1*.csv'))
#for file in csv_files:
#        df = pd.read_csv(file)
#        user.append(df)

-----------OR-------------

#df = pd.read_csv(os.path.join(input_path, 'source1.csv'))
#user.append(df)

-----------OR-------------

df = pd.read_csv('/home/ubuntu/venprabhu/rawdata/source1.csv')
user.append(df)

# Read XML files
#xml_files = glob.glob(os.path.join(input_path, '1*.xml'))
#for file in xml_files:
#        df = pd.read_xml(file)
#        user.append(df)

#df = pd.read_xml(os.path.join(input_path, 'source1.xml'))
#user.append(df)

df = pd.read_xml('/home/ubuntu/venprabhu/rawdata/source1.xml')
user.append(df)

# Read JSON files
#json_files = glob.glob(os.path.join(input_path, '1*.json'))
#for file in json_files:
#        df = pd.read_json(file)
#        user.append(df)


df = pd.read_json('/home/ubuntu/venprabhu/rawdata/source1.json')
user.append(df)


# Combine all DataFrames
combined_df = pd.concat(user, ignore_index=True)


#Step 5: Transform the units and save
transformed_df = convert_units(combined_df)

# Save to CSV in the output directory
transformed_df.to_csv(os.path.join(output_path, 'transformed.csv'), index=False)



# Load Data
# Step 6: Read credentials from environment variables
db_host = os.getenv("DB_HOST")
db_user = os.getenv("DB_USER")
db_password = os.getenv("DB_PASSWORD")
db_port = os.getenv("DB_PORT")
db_name = os.getenv("DB_NAME")


# Step 7: Establish connection to RDS
client = mysql.connector.connect(host=db_host, user=db_user, password=db_password, port=db_port)

# Step 8: Create a cursor
mycursor = client.cursor()

# Step 9: Create the database (if it doesn't exist) and use it
mycursor.execute("CREATE DATABASE IF NOT EXISTS etldb1;")
mycursor.execute("USE eltdb1;")

# Step 10: Create the tables (users)
mycursor.execute("""CREATE TABLE IF NOT EXISTS users (Name VARCHAR(255), height FLOAT, weight FLOAT);""")
client.commit()

# Step 11: Create the engine for SQLAlchemy
engine = sqlalchemy.create_engine(f"mysql+mysqlconnector://{db_user}:{db_password}@{db_host}:{db_port}/etldb")

# Step 12: Read the CSV files into DataFrames(Ignoring as we already have the data in transformed_df dataframe from Step 5)
#transformed_df = pd.read_csv(os.path.join(output_path, '*.csv')     

# Step 13: Upload DataFrames to the respective tables
transformed_df.to_sql("users", engine, if_exists='append', index=False)

# Step 14: Verify the uploads
print(pd.read_sql("SELECT * FROM users;", engine))

# Step 15: Close cursor and connection
mycursor.close()
client.close()
