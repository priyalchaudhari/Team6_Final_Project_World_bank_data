#! /bin/sh
python DataIngestion.py $AWS_ACCESS_KEY $AWS_SECRET_KEY
python TimeSeries_Wrangling.py $AWS_ACCESS_KEY $AWS_SECRET_KEY
python Clustering_Wrangling.py $AWS_ACCESS_KEY $AWS_SECRET_KEY