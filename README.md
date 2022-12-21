<h1 align="center" id="title">Quad-Squad</h1>

<p id="description">Members: Inderjeet Singh(isb5) - Naina Thapar(nta65) - Nidhi Kantekar(nkk9) - Rituraj Ojha(roa11)</p>

  
  
<h2>💻 Built with</h2>

Technologies used in the project:

*   Python
*   SQL
*   Apache Spark
*   PySpark
*   Spark ML
*   Pandas
*   Amazon S3
*   Amazon EMR
*   Amazon EC2
*   Git
*   Tableau
*   Jupyter Notebook



<h2>🛠️ Steps</h2>

Step 1: Create an account on AWS.

Step 2: Create a bucket on Amazon S3.

To create your bucket, do the following steps:
-> Sign on to your IAM id for AWS.
-> Go to the S3 Console:
-> Click Create bucket.
-> In the dialog box, enter:
Bucket name (Text): qsbikesharebucket
AWS Region: US West (Oregon) us-west-2
Accept defaults for all other items
Click Create bucket

Step 2: Upload the 3 datasets and code with similar folder hierarchy as the github repository to the Amazon S3 Bucket.

Step 3: Create a Amazon EMR cluster.


To create the EMR cluster, do the following steps:
-> Cluster name: Cluster_Name
-> Logging (Checkbox): Checked
-> Release: emr-6.8.0
-> Applications: Spark
-> Instance type: m4.2xlarge
-> Number of Instances (Number): 3 (1 master node and 2 work nodes)
-> Click Create cluster.

Step 4: Submit the spark application.

To submit the spark application, do the following steps:
-> Step type: Spark Application
-> Name: Quad-Squad
-> Deploy mode: Client
-> Spark-submit options: --conf spark.yarn.maxAppAttempts=1
-> Application location: s3://qsbikesharebucket/code.py
-> Arguments (Text): s3://qsbikesharebucket/data/final_202122.csv s3://qsbikesharebucket/data/map.csv s3://qsbikesharebucket/data/weatherstats_vancouver_normal_daily.csv s3://qsbikesharebucket/output/final.csv
-> Action on failure: Continue
-> Click ADD

Step 5: Wait for status to change to Completed.

Step 6: You can now be able to see the final.csv file in output folder of your bucket in S3. We used this final.csv file to create the visualizations on Tableau.

Step 7: Run the ml.py code similarly as above, taking final csv file created from above code as input, to run the prediction model.

Step 8: Use the below link to open the Tableau visualizations.



<h2>Links</h2>

Tableau: https://public.tableau.com/app/profile/inderjeet.singh.bhatti/viz/Mobi-ShawAnalysis/Story1

Quad-Squad Folder: https://drive.google.com/drive/folders/1fHlwhwYc3W-pyujbtNykRBT_IqMlReCU

Mobi Shaw Dataset (large size so not uploaded in github): https://drive.google.com/file/d/1ILN_LhfbIcYGZOE0LZfeV1wNxJB572Vh/view?usp=share_link

Video Link: https://drive.google.com/file/d/181IB_-2IsTnqssDIr_p015SnOFIntURu/view?usp=share_link
