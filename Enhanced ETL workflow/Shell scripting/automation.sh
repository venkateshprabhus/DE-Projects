#Fetcting data from the web URL
wget -O /home/ubuntu/venprabhu/rawdata/raw_data.zip https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBMDeveloperSkillsNetwork-PY0221EN-SkillsNetwork/labs/module%206/Lab%20-%20Extract%20Transform%20Load/data/source.zip
if [ $? -ne 0 ]; then
	    echo "Error extracting the file."
	        exit 1
fi

#unzipping the file to a separate folder
unzip /home/ubuntu/venprabhu/rawdata/raw_data.zip -d /home/ubuntu/venprabhu/rawdata/
if [ $? -ne 0 ]; then
	    echo "Error unzipping the file."
	        exit 1
fi


#uploading the raw files to S3
aws s3 cp /home/ubuntu/venprabhu/rawdata s3://etl-workflows/raw_data --recursive
if [ $? -ne 0 ]; then
	    echo "Error uploading raw file to S3."
	        exit 1
fi

#Transforming the data and loading transformed data to RDS database
python transform_load_data.py
if [ $? -ne 0 ]; then
	    echo "Error in transform_load_data.py file."
	        exit 1
fi

#Loading the transformed data to S3
aws s3 cp /home/ubuntu/venprabhu/processed s3://etl-workflows/transformed_data --recursive
if [ $? -ne 0 ]; then
	    echo "Error loading transformed file to S3."
	        exit 1
fi

