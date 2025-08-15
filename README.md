# alterdataInterview
This repository contains a cloud function developed as a part of the interview process for the position of Senior Data 
Engineer at Alterdata.

## Exercise Overview
The task is to write a Python script extracting air quality data from the public [OpenAQ API](https://docs.openaq.org/ ) 
and storing it in a GCS bucket. The script should extract the latest PM2.5, PM10, NO2, and O3 data for two selected 
cities (e.g. Warsaw and London). 

1. Functional Requirements - the script should:
a. Download the latest available measurement data for the following parameters: PM2.5, PM10, O3, and NO2 for the selected cities.
b. Integrate data from at least three different measurement stations for each city, if available.
c. Save the results in a selected format (e.g. CSV), where each element/row represents a single reading from a specific station containing: city name, station location, parameter, value, unit, and measurement time.
d. Add simple data validation and error handling.
e. Consider using the script in an automated solution that periodically loads the data warehouse.

2. Technical Requirements and Runtime Environment:
a. The data processing script should be implemented as a Google Cloud Function.
b. The resulting file containing the processed data must be loaded into a bucket in Google Cloud Storage (GCS).

## Cloud Function Deployment
Cloud function can be deployed by calling the command below from the cloud_function/ directory.
```gcloud
gcloud functions deploy openaq_data_download --gen2 --runtime=python312 --region=europe-central2 --source=. --entry-point=openaq_data_download --set-env-vars API_KEY=YOUR_API_KEY --trigger-http
```

## Potential Improvements
1. Setting up Cloud Scheduler to trigger the cloud function on schedule to keep the data source up to date. This
approach was not pursued as the administrator did not provide access to Cloud Scheduler.
2. API_KEY should be stored in Secrets Manager instead of being passed to the cloud function as an environment variable.
This approach was not pursued as the administrator did not provide access to Secrets Manager.
3. Deploying cloud function and cloud scheduler as code, perhaps changing cloud function trigger to Pub/Sub. This
approach was not pursued due to missing permissions.
