# -*- coding: utf-8 -*-
"""
Created on Fri May 29 10:09:12 2020

@author: Jenil Patel
"""

import psycopg2
import pandas as pd
import re

def extract_data(conn, query):
    """ query relevant columns from the table """

    try:
        cur = conn.cursor()
        cur.execute(query)
        
        print("The number of parts: ", cur.rowcount)
        
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


conn = psycopg2.connect(host="localhost", database="job_info", user="postgres", password="admin")

query = ''' SELECT "Job Title" AS job_title, "Job Description" AS job_desc,
            "Location", "Size", "Founded", "Type of ownership" AS ownership,
            "Industry", "Sector", "Revenue" FROM data_scientist  '''

data = extract_data(conn, query)





def transform_data(temp):
    
    data = temp.copy()
    
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
    
    data['job_title'] = data['job_title'].apply(lambda x:'Engineer' if re.search('engineer|architect|developer', x.lower()) else x)
    
    data['job_title'] = data['job_title'].apply(lambda x:'Scientist' if re.search('scientist|science|modeler|specialist', x.lower()) else x) 
    
    print(data['job_title'].unique())
    print(data['job_title'].nunique())
    

transform_data(data)






















