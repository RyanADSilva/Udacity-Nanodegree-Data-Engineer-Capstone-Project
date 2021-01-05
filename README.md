# Insights - US Immigration
### Data Engineering Capstone Project

#### Project Summary
The goal of the project is to determine 
* The US states which are receiving the highest number of immigrants by year and then by month to see if there is a change during certain months. 
* Finding ports that are receiving the most traffic and the type of travelers / visa types arriving at these ports , 
* Finding gender and age ranges arriving at these states. 
* Finding airlines that are bringing in the most immigrants


#### The project follows the follow steps:
* Step 1: Scope the Project and Gather Data
* Step 2: Explore and Assess the Data
* Step 3: Define the Data Model
* Step 4: Run ETL to Model the Data
* Step 5: Complete Project Write Up

------


### Step 1: Scope the Project and Gather Data

#### Scope 

The plan is to primarly use the i94 immigration data , supplement it with the demographics and airport metadata to arrive at our findings. 

* The plan would be to extract data from the immigration files , transform it and store it as parquet files for easy access. 
* We will use Spark to perform this operation and then transforming the data into  fact and dimention files and store them as parquet files.
* We will build a data lake of necessary data and then extract and aggregate the data using Spark to perform analysis of the data.



#### Describe and Gather Data 

* `I94 Immigration Data`: This data comes from the US National Tourism and Trade Office. A data dictionary is included in the workspace. This is where the data comes from [!https://travel.trade.gov/research/reports/i94/historical/2016.html] There's a sample file so you can take a look at the data in csv format before reading it all in. 
    * The file contains a record of each i94 issued along with the relevant particulars of that i94. 
    

* `U.S. City Demographic Data:` This data comes from OpenSoft. 
You can read more about it here.[!https://public.opendatasoft.com/explore/dataset/us-cities-demographics/export/]
    * This file contains the demographics per city within a state.

* `Airport Code Table`: This is a simple table of airport codes and corresponding cities. 
It comes from here [!https://datahub.io/core/airport-codes#data]
    * This file contains a record per airport with all relevant IATA particulars.
    
--

### Step 2: Explore and Assess the Data
#### Explore the Data 

* `I94 Immigration Data`: 
    * Each record did have the port of entry , year and month information.  
    * There were records where the i94addr and gender were empty.
    * The arrival and departure date columns were numbers and not datetime. 
    * Each record could be uniquely identifies by the column admnum which was numberic.

* `U.S. City Demographic Data:`
    * Record was presented at the city level. 
    * Decided to normalize the data and create a State dimension from this file of unique states.

* `Airport Code Table`
    * Record was presented for each report across the world with all relevant IATA information. 

#### Cleaning Steps
* `I94 Immigration Data`: 
    * `Assumption`: The port of entry and the i94addr are in the same state, as we did not have a proper file to reference the relationship other than the data dictionairy.
    * Checked and Dropped duplicate records.
    * Converted a few columns into numeric format from string , as we wanted to partition the data by those columns ( i94 year and month )
    * Added additional datetime columns based on given arrival date and departure date . We added these colummns as it will help us drill down to a more granular level if needed more than just the month . This will help us help us aggregate immigration by week and weekday.  
    
* `U.S. City Demographic Data:`
    * Dropped duplicate records
    * Created a state dimension of unique state records.
    
* `Airport Code Table`:
    * Dropped duplicate records
    * Filtered only for US airports as we are only dealing with US immigration.
    * Add a state column to the table to help join to the table

------

### Step 3: Define the Data Model

I have chosen a `Relational data model with a star schema` for our solution for the following reasons
* Most of the fact data is present in the immigration data file . Data for most query analysis will be sourced from this fact table.
* We have multiple sources of data files which make up our dimensions. Isolating the ETL for these files removes interdependency on saving the data.
* By using the star schema we are flexible to use different and only relevant data sources using JOIN operators to generate our query analysis.
* The data in the dimension tables is relatively small as compared the the fact immigration data.

#### 3.1 Conceptual Data Model

![Data Model](DataModel.PNG)

* `Immg_data`: This table will hold all i94 details of an immigrant. While creating the paquet file we have partitioned the data by Year and then month for easy retrieval in the future . 

* `States` : A dimension table which contain all information about states in US . The information for this dimension will
be derived from the demographics file. 

* `Times` : This dimension table will store the disctict dates derived from the arrival and departure dates in thre immgartion file . This dimension will help us perofrm a more granualr analysis on immigration data if required as it is at a per day level.  

* `Demographics` : This dimension will store all the demographic information at a city level . 

* `Airports` : This dimension table will maintain a record of all airports within  a particular state in the US along with relevant IATA information about the airport.

#### 3.2 Mapping Out Data Pipelines

* Create a seperate python file "etl.py" which will contain the entire data pipeline.

* Create a function to load the demographics data . After eliminating duplicate records and checking the columns data ,we will create a state dimension and a demograpic dimension and store this data into parquet files.

* Create a function to load the airport data . After eliminating duplicate records and fileter out for only US airports ,we will create a airports dimension and store this data into parquet files. Add an additional column for state to this dimension.

* Create a function to load the immigration data . After eliminating duplicate records and checking the columns data ,we will create a immigration fact and store this data into parquet files partitioned by year and month. Even though we will not be using all the data we will still save it if in furure we require this data.
    * We will extract the arrival date and departure date from the immigration file and create a seperate time dimension and store this dimension as a seperate parquet file .

* Create a function to perform the data quality checks once the pipeline have created all the parquet files.

### Step 4: Run Pipelines to Model the Data 
#### 4.1 Create the data model
The entire pipeline has been built using a seperate file `etl.py` located in this project.

##### How to use
* Need to substitute the variable `input_data` with the location where all the files /S3 folder location where the files reside.
* Need to substitute the variable `output_data` with the location where all the folder/S3 folder location where the output parquet files will be stored.
* In function `process_immig_data` replace the `datafile` valiable value with the file you are going to process.

* run the script etl.py


--

#### 4.2 Data Quality Checks
Run Quality Checks
* Check if records are present in the dimension parquet files like states and airports
* Check if the immigration table contains any null columns for certain important columns like i94 year and month 
* Check foreign key relationship between the immigration table and states.
* Create a framework whereby it is possible to add test cases 
--
#### 4.3 Data dictionary 
Included a file named "DataDictionary.tab" in this project . The file is a tab seperated file of columns [Table], [Column Name] and [Description].

#### Step 5: Complete Project Write Up
* `Clearly state the rationale for the choice of tools and technologies for the project.`
    * The goal was to leverage the power of `Spark` to extract data from a data lake to perform our analysis.
    * By using this technology we are able to load and aggregate data quickly without being tied to developing a traditional datawarehouse model and hence in turn are able to derive faster analytics.  
    * `Spark` is 100 times faster than MapReduce. 
    * `Spark` accelerates delivery of insight with in-memory processing across a distributed framework.
    * The value of Spark lies in its ability to enable more people than ever to collaborate when accessing data, applying analytics and deploying deep intelligence in every type of application: Internet of Things, web, mobile, social, business process and others.
    *  `Spark` abstracts complexity of data access across countless landing zones such as the Hadoop Distributed File System (HDFS), relational databases, fast-moving data streams, distributed file systems and much more.
    * `Spark` supports a number of declarative programming languages such as Python and Scala. This support, combined with the ability to work with a multitude of different data sources, helps make life much easier than ever for data science professionals. A number of integrated, high-level tools for machine learning and streaming data helps further reduce development time and helps foster building highly intelligent applications.
    * `Spark` helps data scientists by supporting the entire data science workflow, from data access and integration to machine learning and visualization using the language of choice—which is typically Python. It also provides a growing library of machine-learning algorithms through its machine-learning library (MLlib).
    
* `Propose how often the data should be updated and why.`
    * Assuming that get a huge amout of immigration data , we can plan to load each months immigration data file at a time , thus performing an incremental data addiction on our fact table. 
* `Write a description of how you would approach the problem differently under the following scenarios:`
 * `The data was increased by 100x.`
     * Since we are using Spark for this project , we could increase the nodes of our Spark cluster so that we have more worker threads to process and speed up the processing. 
     * A further aggregation of data based on Usage and additional summarized parquet files stored 
     * Moving and storing the aggregations in a MPP datawarehouse like Redshift and then retrieving the data from the Redshift database itself
     * Moving towards an incremental load of data with smaller date durations.
 * `The data populates a dashboard that must be updated on a daily basis by 7am every day.`
     * Implementing a scheduled pipeline using Airflow to load previous days delta files. 
     * Moving and storing the aggregations in a MPP datawarehouse like Redshift and then retrieving the data from the Redshift database itself to display the dashboard     
 * `The database needed to be accessed by 100+ people.`
      * A further aggregation of data based on Usage and additional summarized parquet files stored which will be loaded into Redshift.
      * Moving and storing the aggregations in a MPP datawarehouse like Redshift and then retrieving the data from the Redshift database itself
      * Moving towards an incremental load of data with smaller date durations so that we are able to load the data quicker.    
      * Saving the parquet files in Amazon S3 will help speed up concurrent requests because of the quick i/o requests/secs of AWS S3.
      