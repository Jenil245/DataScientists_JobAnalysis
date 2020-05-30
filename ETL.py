# -*- coding: utf-8 -*-
"""
Created on Fri May 29 10:09:12 2020

@author: Jenil Patel
"""

import psycopg2
import pandas as pd
import re

#Let's extract required data from postgres database.
def extract_data(conn, query):
    """ query relevant columns from the table """

    try:
        cur = conn.cursor()
        cur.execute(query)
        
        rows = cur.fetchall()
        column_names = [item[0] for item in cur.description]      
    
        df = pd.DataFrame(rows, columns= column_names)

        cur.close()
    
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    
    finally:
        if conn is not None:
            conn.close()
            
    return df


#Let's transform data into a form which can be used to derive insights.
def transform_data(temp_data):
    
    data = temp_data.copy()
    
    #job_title 
    '''there are total 308 different job titles Because Companies often put 
    various senority level and keyword related to thier industry like 'environmental data scientist'
    
    It does explain why most candidate feel confused about data science role.
    It would not make any sense with these many level therefore we will categorize job_title into specific role
    such as Data Scientist, Data Analyst, Data Engineer, Research Scientist, ML engineer, Manager'''
    
    
    data['job_title'] = data['job_title'].apply(lambda x:'Manager' if re.search('manager|director', x.lower())  else x) 
    
    data['job_title'] = data['job_title'].apply(lambda x:'Researcher' if re.search('research', x.lower()) else x) 
    
    data['job_title'] = data['job_title'].apply(lambda x:'Analyst' if re.search('analyst|analytics', x.lower()) else x) 
    
    data['job_title'] = data['job_title'].apply(lambda x:'Machine Learning Eng' if re.search('machine', x.lower()) else x) 
    
    data['job_title'] = data['job_title'].apply(
            lambda x:'Engineer' if re.search('engineer|architect|developer', x.lower()) else x)
    
    data['job_title'] = data['job_title'].apply(
            lambda x:'Scientist' if re.search('scientist|science|modeler|specialist', x.lower()) else x) 
    
    
    #job_desc
    '''
    Let's see which tools and languages are mentioned in job descriptions which will help Aspiring Data Scientist
    to decide which things to learn
    '''
    
    ###python
    data['Python'] = data['job_desc'].apply(lambda x:1 if re.search('python', x.lower()) else 0)
    
    ###R-language
    data['R'] = data['job_desc'].apply(
            lambda x:1 if re.search('r language|r-studio|r studio|r programming|statistical programming', x.lower()) else 0)
    
    ###SAS
    data['SAS'] = data['job_desc'].apply(lambda x:1 if re.search('statistical software|statistical tool', x.lower()) else 0)
    
    ###scala
    data['Scala'] = data['job_desc'].apply(lambda x:1 if re.search('scala', x.lower()) else 0)
    
    ###spark
    data['Spark'] = data['job_desc'].apply(lambda x:1 if re.search('spark|distributed programming', x.lower()) else 0)
    
    ###AWS
    data['AWS'] = data['job_desc'].apply(lambda x:1 if re.search('aws|cloud', x.lower()) else 0)
    
    ###SQL
    data['SQL'] = data['job_desc'].apply(lambda x:1 if re.search('sql|query language', x.lower()) else 0)
    
    ###Excel
    data['Excel'] = data['job_desc'].apply(lambda x:1 if re.search('excel|microsoft office', x.lower()) else 0)
    
    ###power_bi
    data['Power_BI'] = data['job_desc'].apply(lambda x:1 if re.search('power bi|microsoft bi', x.lower()) else 0)
    
    ###Tableau
    data['Tableau'] = data['job_desc'].apply(lambda x:1 if re.search('tableau', x.lower()) else 0)
    
    ###Tensorflow
    data['Tensorflow'] = data['job_desc'].apply(lambda x:1 if re.search('tensorflow', x.lower()) else 0)
    
    ###pytorch
    data['Pytorch'] = data['job_desc'].apply(lambda x:1 if re.search('pytorch', x.lower()) else 0)
    
    #size
    '''
    Categorizing company size into three parts large, medium, small.
    '''
    
    data['Company_Size'] = data['Size'].apply(lambda x: x.replace('employees','').replace('+','').replace('Unknown','1'))
    
    data['Company_Size'] = data['Company_Size'].apply(lambda x: x.split('to')[0]).astype(int).apply(
            lambda x: 'Small' if x < 200 else ('Medium' if x < 10000 else 'Large'))
    
    #Founded
    '''
    Let's calculate companies age which will help us analyse Does new comapnies hire data scientists?
    '''
    
    data['Company_Age'] = data['Founded'].apply(lambda x:x if x<0 else 2020-x)
    
    data.drop(columns=['job_desc', 'Size', 'Founded', 'ownership', 'Industry', 'Revenue'], inplace=True)
    
    return data


#Let's load data into csv file which can be easily used by others for analysis and visualization
def load_data(data, path=""):
    data.to_csv(path+"data_scientist_jobinfo.csv", index=False)
    print("CSV file is created Successfully")



if __name__ == "__main__": 
    
    conn = psycopg2.connect(host="localhost", database="job_info", user="postgres", password="admin")

    query = ''' SELECT "Job Title" AS job_title, "Job Description" AS job_desc,
            "Location", "Size", "Founded", "Type of ownership" AS ownership,
            "Industry", "Sector", "Revenue" FROM data_scientist  '''
            
    #extract data from postgres_db        
    data = extract_data(conn, query)
    
    #transform it to analyse effectively
    t_data = transform_data(data)

    #load data into csv file for easy accessing
    load_data(t_data)
    
    