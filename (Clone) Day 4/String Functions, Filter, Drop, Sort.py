# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

employees = [(1, "Scott", "Tiger", 1000.0, 
                      "united states", "+1 123 456 7890", "123 45 6789"
                     ),
                     (2, "Henry", "Ford", 1250.0, 
                      "India", "+91 234 567 8901", "456 78 9123"
                     ),
                     (3, "Nick", "Junior", 750.0, 
                      "united KINGDOM", "+44 111 111 1111", "222 33 4444"
                     ),
                     (4, "Bill", "Gomes", 1500.0, 
                      "AUSTRALIA", "+61 987 654 3210", "789 12 6118"
                     )
                ]
employeesDF = spark. \
    createDataFrame(employees,
                    schema="""employee_id INT, first_name STRING, 
                    last_name STRING, salary FLOAT, nationality STRING,
                    phone_number STRING, ssn STRING"""
                   )
display(employeesDF)

# COMMAND ----------

help(upper)

# COMMAND ----------

employeesDF.withColumn("Nationalitynew", upper("nationality")).show()

# COMMAND ----------

Titlecase-- initcap
lowercase
uppercase

all in one df

# COMMAND ----------

df2=employeesDF.select("employee_id","nationality").withColumn("upper", upper("nationality"))\
    .withColumn("lower",lower("nationality"))\
    .withColumn("init",initcap("nationality"))

# COMMAND ----------

Task: Extract last 4 digits from SSN.

hint(split, substring)

Task: Extract country code and area code from phone no column

# COMMAND ----------

help(substring)

# COMMAND ----------

employeesDF.withColumn("last4ssn",substring("ssn",-4,4).cast("int")).show()

# COMMAND ----------

help(split)

# COMMAND ----------

employeesDF.withColumn("countrycode",split("phone_number"," ")).display()

# COMMAND ----------

employeesDF.withColumn("countrycode",split("phone_number"," ")[0])\
.withColumn("areacode",split("phone_number"," ")[1])\
.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Dataframe functions
# MAGIC - Filter or where

# COMMAND ----------

help(employeesDF.filter)

# COMMAND ----------

employeesDF.filter("employee_id=1").show()

# COMMAND ----------

employeesDF.where("employee_id=1").show()

# COMMAND ----------

employeesDF.where(col("employee_id")==1).show()

# COMMAND ----------

employeesDF.select("employee_id".alias("emp_id")).show()

# COMMAND ----------

employeesDF.select(col("employee_id").alias("emp_id")).show()

# COMMAND ----------

employeesDF.show()

# COMMAND ----------

employeesDF.filter(col("salary").between(1000,1500)).show()

# COMMAND ----------

employeesDF.filter("salary BETWEEN 1000 and 1500").show()

# COMMAND ----------

employeesDF.filter((col("nationality")=="India") & (col("salary")==1250)).show()

# COMMAND ----------

employeesDF.filter("nationality='India' AND salary=1250").show()

# COMMAND ----------

employeesDF.display()

# COMMAND ----------

df1=employeesDF.withColumn("Nationality",initcap("nationality")).drop("phone_number")

# COMMAND ----------

df1.drop("ssn").show()

# COMMAND ----------

df1.display()

# COMMAND ----------

help(employeesDF.orderBy)

# COMMAND ----------

employeesDF.sort("nationality").show()

# COMMAND ----------

employeesDF.sort("nationality",ascending=False).show()

# COMMAND ----------

employeesDF.sort("nationality".desc()).show()

# COMMAND ----------

employeesDF.sort(col("nationality").desc()).show()

# COMMAND ----------

employeesDF.sort(desc("nationality")).show()

# COMMAND ----------

courses = [{'course_id': 1,
  'course_name': '2020 Complete Python Bootcamp: From Zero to Hero in Python',
  'suitable_for': 'Beginner',
  'enrollment': 1100093,
  'stars': 4.6,
  'number_of_ratings': 318066},
 {'course_id': 4,
  'course_name': 'Angular - The Complete Guide (2020 Edition)',
  'suitable_for': 'Intermediate',
  'enrollment': 422557,
  'stars': 4.6,
  'number_of_ratings': 129984},
 {'course_id': 12,
  'course_name': 'Automate the Boring Stuff with Python Programming',
  'suitable_for': 'Advanced',
  'enrollment': 692617,
  'stars': 4.6,
  'number_of_ratings': 70508},
 {'course_id': 10,
  'course_name': 'Complete C# Unity Game Developer 2D',
  'suitable_for': 'Advanced',
  'enrollment': 364934,
  'stars': 4.6,
  'number_of_ratings': 78989},
 {'course_id': 5,
  'course_name': 'Java Programming Masterclass for Software Developers',
  'suitable_for': 'Advanced',
  'enrollment': 502572,
  'stars': 4.6,
  'number_of_ratings': 123798},
 {'course_id': 15,
  'course_name': 'Learn Python Programming Masterclass',
  'suitable_for': 'Advanced',
  'enrollment': 240790,
  'stars': 4.5,
  'number_of_ratings': 58677},
 {'course_id': 3,
  'course_name': 'Machine Learning A-Z™: Hands-On Python & R In Data Science',
  'suitable_for': 'Intermediate',
  'enrollment': 692812,
  'stars': 4.5,
  'number_of_ratings': 132228},
 {'course_id': 14,
  'course_name': 'Modern React with Redux [2020 Update]',
  'suitable_for': 'Intermediate',
  'enrollment': 203214,
  'stars': 4.7,
  'number_of_ratings': 60835},
 {'course_id': 8,
  'course_name': 'Python for Data Science and Machine Learning Bootcamp',
  'suitable_for': 'Intermediate',
  'enrollment': 387789,
  'stars': 4.6,
  'number_of_ratings': 87403},
 {'course_id': 6,
  'course_name': 'React - The Complete Guide (incl Hooks, React Router, Redux)',
  'suitable_for': 'Intermediate',
  'enrollment': 304670,
  'stars': 4.6,
  'number_of_ratings': 90964},
 {'course_id': 18,
  'course_name': 'Selenium WebDriver with Java -Basics to Advanced+Frameworks',
  'suitable_for': 'Advanced',
  'enrollment': 148562,
  'stars': 4.6,
  'number_of_ratings': 49947},
 {'course_id': 21,
  'course_name': 'Spring & Hibernate for Beginners (includes Spring Boot)',
  'suitable_for': 'Advanced',
  'enrollment': 177053,
  'stars': 4.6,
  'number_of_ratings': 45329},
 {'course_id': 7,
  'course_name': 'The Complete 2020 Web Development Bootcamp',
  'suitable_for': 'Beginner',
  'enrollment': 270656,
  'stars': 4.7,
  'number_of_ratings': 88098},
 {'course_id': 9,
  'course_name': 'The Complete JavaScript Course 2020: Build Real Projects!',
  'suitable_for': 'Intermediate',
  'enrollment': 347979,
  'stars': 4.6,
  'number_of_ratings': 83521},
 {'course_id': 16,
  'course_name': 'The Complete Node.js Developer Course (3rd Edition)',
  'suitable_for': 'Advanced',
  'enrollment': 202922,
  'stars': 4.7,
  'number_of_ratings': 50885},
 {'course_id': 13,
  'course_name': 'The Complete Web Developer Course 2.0',
  'suitable_for': 'Intermediate',
  'enrollment': 273598,
  'stars': 4.5,
  'number_of_ratings': 63175},
 {'course_id': 11,
  'course_name': 'The Data Science Course 2020: Complete Data Science Bootcamp',
  'suitable_for': 'Beginner',
  'enrollment': 325047,
  'stars': 4.5,
  'number_of_ratings': 76907},
 {'course_id': 20,
  'course_name': 'The Ultimate MySQL Bootcamp: Go from SQL Beginner to Expert',
  'suitable_for': 'Beginner',
  'enrollment': 203366,
  'stars': 4.6,
  'number_of_ratings': 45382},
 {'course_id': 2,
  'course_name': 'The Web Developer Bootcamp',
  'suitable_for': 'Beginner',
  'enrollment': 596726,
  'stars': 4.6,
  'number_of_ratings': 182997},
 {'course_id': 19,
  'course_name': 'Unreal Engine C++ Developer: Learn C++ and Make Video Games',
  'suitable_for': 'Advanced',
  'enrollment': 229005,
  'stars': 4.5,
  'number_of_ratings': 45860},
 {'course_id': 17,
  'course_name': 'iOS 13 & Swift 5 - The Complete iOS App Development Bootcamp',
  'suitable_for': 'Advanced',
  'enrollment': 179598,
  'stars': 4.8,
  'number_of_ratings': 49972},
 {'course_id': None,
  'course_name':None,
  'suitable_for': None,
  'enrollment': None,
  'stars': None,
  'number_of_ratings': None},
 {'course_id': None,
  'course_name':None,
  'suitable_for': None,
  'enrollment': None,
  'stars': None,
  'number_of_ratings': None},
 {'course_id': None,
  'course_name':None,
  'suitable_for': None,
  'enrollment': None,
  'stars': None,
  'number_of_ratings': None},
 ]

# COMMAND ----------

df=spark.createDataFrame(courses)

# COMMAND ----------

df.show()

# COMMAND ----------

display(df)

# COMMAND ----------

df.orderBy(col("course_id").asc_nulls_last()).display()

# COMMAND ----------

df1=df.dropna()

# COMMAND ----------

df1.sort("suitable_for", col("enrollment").desc()).show(truncate=False)

# COMMAND ----------


