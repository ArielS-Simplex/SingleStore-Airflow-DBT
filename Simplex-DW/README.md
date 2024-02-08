# Simplex-DW
Get started
git clone https://github.com/SimplexCC/Simplex-DW.git
cd Simplex-DW
Step one: Building the Docker image
Build the Docker container image using the following command:

./mwaa-local-env build-image
Note: it takes several minutes to build the Docker image locally.

Step two: Running Apache Airflow
Local runner
Runs a local Apache Airflow environment that is a close representation of MWAA by configuration.

./mwaa-local-env start
To stop the local environment, Ctrl+C on the terminal and wait till the local runner and the postgres containers are stopped.

Step three: Accessing the Airflow UI
By default, the bootstrap.sh script creates a username and password for your local Airflow environment.

Username: admin
Password: test
Airflow UI
Open the Apache Airlfow UI: http://localhost:8080/.
Step four: Add DAGs and supporting files
The following section describes where to add your DAG code and supporting files. We recommend creating a directory structure similar to your MWAA environment.

DAGs
Add DAG code to the dags/ folder.
To run the sample code in this repository, see the example_dag_with_taskflow_api.py file.
Requirements.txt
Add Python dependencies to requirements/requirements.txt.
To test a requirements.txt without running Apache Airflow, use the following script:
./mwaa-local-env test-requirements
Let's say you add aws-batch==0.6 to your requirements/requirements.txt file. You should see an output similar to:

Installing requirements.txt
Collecting aws-batch (from -r /usr/local/airflow/dags/requirements.txt (line 1))
  Downloading https://files.pythonhosted.org/packages/5d/11/3aedc6e150d2df6f3d422d7107ac9eba5b50261cf57ab813bb00d8299a34/aws_batch-0.6.tar.gz
Collecting awscli (from aws-batch->-r /usr/local/airflow/dags/requirements.txt (line 1))
  Downloading https://files.pythonhosted.org/packages/07/4a/d054884c2ef4eb3c237e1f4007d3ece5c46e286e4258288f0116724af009/awscli-1.19.21-py2.py3-none-any.whl (3.6MB)
    100% |████████████████████████████████| 3.6MB 365kB/s 
...
...
...
Installing collected packages: botocore, docutils, pyasn1, rsa, awscli, aws-batch
  Running setup.py install for aws-batch ... done
Successfully installed aws-batch-0.6 awscli-1.19.21 botocore-1.20.21 docutils-0.15.2 pyasn1-0.4.8 rsa-4.7.2
To package the necessary WHL files for your requirements.txt without running Apache Airflow, use the following script:
./mwaa-local-env package-requirements
For example usage see Installing Python dependencies using PyPi.org Requirements File Format Option two: Python wheels (.whl).

Custom plugins
There is a directory at the root of this repository called plugins.
In this directory, create a file for your new custom plugin.
Add any Python dependencies to requirements/requirements.txt.
Note: this step assumes you have a DAG that corresponds to the custom plugin. For example usage MWAA Code Examples.

Startup script
There is a sample shell script startup.sh located in a directory at the root of this repository called startup_script.
If there is a need to run additional setup (e.g. install system libraries, setting up environment variables), please modify the startup.sh script.
To test a startup.sh without running Apache Airflow, use the following script:
./mwaa-local-env test-startup-script
What's next?
Learn how to upload the requirements.txt file to your Amazon S3 bucket in Installing Python dependencies.
Learn how to upload the DAG code to the dags folder in your Amazon S3 bucket in Adding or updating DAGs.
Learn more about how to upload the plugins.zip file to your Amazon S3 bucket in Installing custom plugins.
FAQs
The following section contains common questions and answers you may encounter when using your Docker container image.

Can I test execution role permissions using this repository?
You can setup the local Airflow's boto with the intended execution role to test your DAGs with AWS operators before uploading to your Amazon S3 bucket. To setup aws connection for Airflow locally see Airflow | AWS Connection To learn more, see Amazon MWAA Execution Role.
You can set AWS credentials via environment variables set in the docker/config/.env.localrunner env file. To learn more about AWS environment variables, see Environment variables to configure the AWS CLI and Using temporary security credentials with the AWS CLI. Simply set the relevant environment variables in .env.localrunner and ./mwaa-local-env start.
How do I add libraries to requirements.txt and test install?
A requirements.txt file is included in the /requirements folder of your local Docker container image. We recommend adding libraries to this file, and running locally.
What if a library is not available on PyPi.org?
If a library is not available in the Python Package Index (PyPi.org), add the --index-url flag to the package in your requirements/requirements.txt file. To learn more, see Managing Python dependencies in requirements.txt.
Troubleshooting
The following section contains errors you may encounter when using the Docker container image in this repository.

My environment is not starting
If you encountered the following error: process fails with "dag_stats_table already exists", you'll need to reset your database using the following command:
./mwaa-local-env reset-db
If you are moving from an older version of local-runner you may need to run the above reset-db command, or delete your ./db-data folder. Note, too, that newer Airflow versions have newer provider packages, which may require updating your DAG code.
Fernet Key InvalidToken
A Fernet Key is generated during image build (./mwaa-local-env build-image) and is durable throughout all containers started from that image. This key is used to encrypt connection passwords in the Airflow DB. If changes are made to the image and it is rebuilt, you may get a new key that will not match the key used when the Airflow DB was initialized, in this case you will need to reset the DB (./mwaa-local-env reset-db).

