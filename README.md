### Data modelling and ETL using AWS Glue
This repo. contains the code to implement a ETL pipeline which extracts data from AWS S3, transforms them to a analytic database and load them back into the AWS S3. The whole pipeline is implemented in AWS Glue. It simulates a fictious scenario set out in the UDacity Data Engineering with AWS Nanodegree. 

### Project Background: STEDI S3 and Glue ETL
The STEDI Team has been hard at work developing a hardware STEDI Step Trainer that trains the user to do a STEDI balance exercise. The hardware device has sensors on the device that collect data to train a machine-learning algorithm to detect steps. A user uses a companion mobile app that collects customer data and interacts with the device sensors. Several customers have already received their Step Trainers, installed the mobile application, and begun using them together to test their balance. The Step Trainer is just a motion sensor that records the distance of the object detected. The app uses a mobile phone accelerometer to detect motion in the X, Y, and Z directions.

The STEDI team wants to use the motion sensor data to train a machine learning model to detect steps accurately in real-time. Privacy will be a primary consideration in deciding what data can be used. Some of the early adopters have agreed to share their data for research purposes. Only these customers’ Step Trainer and accelerometer data should be used in the training data for the machine learning model. 

In this project, the data produced by the STEDI Step Trainer sensors and the mobile app is extracted and then they are curated into a data lakehouse solution on AWS so that Data Scientists can train the learning model.

### Implementations
All the implementations are run in AWS Glue as a series of Spark jobs. The codes in this project only show the scripted Glue jobs and data definitions.  
