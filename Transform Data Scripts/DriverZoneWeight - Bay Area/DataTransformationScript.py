import pandas as pd
import os
import pymysql as sql

filename = 'C:\Wingz\Transform Data Scripts\Wingz Driver Ride Distribution Preference Questionnaire  - Bay Area.csv'
filename = os.path.abspath(filename)
df = pd.read_csv(filename)

#get database user id and email
conn = sql.connect(host='wingz-platform-read001.c8voyumknq5z.us-west-1.rds.amazonaws.com',
                            user='wingz-read-only', password='4P4v53S256hW7Z2X',
                            db='wingz-prod')
def query(query):
    return pd.read_sql(query, con=conn)
dfQuery = """
select id_utilisateur, email from utilisateurs 
"""
temp = query(dfQuery)    

#add all user id's to email
df.rename(columns={'Wingz Email Address':'email'}, inplace=True)
df = df.merge(temp, on = 'email',how='left')
                        

new_df = pd.DataFrame(columns=['email_utilisateur','id_zone','id_utilisateur','weight'])

df['Your Geo Zone Selection [San Mateo County]'] = df['Unnamed: 16']
df['Your Geo Zone Selection [San Francisco County]'] = df['Unnamed: 17']

df.drop(['Unnamed: 16', 'Unnamed: 17'], axis = 1, inplace= True)

k = 0
zones = ['Your Geo Zone Selection [Lake County]','Your Geo Zone Selection [Sonoma County]','Your Geo Zone Selection [Marin County]', 'Your Geo Zone Selection [Napa County]',
         'Your Geo Zone Selection [Solano County]', 'Your Geo Zone Selection [Contra Costa County]',
'Your Geo Zone Selection [Alameda County]', 'Your Geo Zone Selection [Santa Clara County]', 'Your Geo Zone Selection [Santa Cruz County]',
'Your Geo Zone Selection [San Mateo County]','Your Geo Zone Selection [San Francisco County]']

def weight_to_int(weight):
    if weight == 'Preferred':
        return 3
    if weight == 'Neutral':
        return 2
    if weight == 'Rejected':
        return 1

for i in range(len(df)):
    for j in zones:
        new_df.loc[k] = [df.iloc[i]['email'],j,df.iloc[i]['id_utilisateur'],weight_to_int(df.iloc[i][j])]
        k += 1
        
new_df.to_csv('C:\Wingz\Transform Data Scripts\DriverZoneWeight - Bay Area\driver_weights.csv')