import pandas as pd
import requests
import os

#create file path
pathname = 'C://Users\Michael Z\Downloads\stuff'
filename = 'premium_no_mvr.csv'
filepath = os.path.join(pathname, filename)

#read csv file into dataframe
premium_no_mvr = pd.read_csv(filepath)



x = []
for y in list(zip(premium_no_mvr["Candidate ID"], premium_no_mvr["Completed at"])):
    x.append(y)


for candidate_id, date in x:
    key = "9938d82c228558cdebf0bdbc06ab6dab6a4f8988"
    query_link = "https://api.checkr.com/v1/subscriptions/4722c07dd9a10c3985ae432a"
    r = requests.delete(query_link, auth=(key, ""), data = {'candidate_id':candidate_id, 'package':'mvr_only', 'interval_count':'3', 'interval_unit': 'month', 'start_date':date})
    j = r.json()

candidate_id = 'f2da2c03518158fb8a1ab6b4'
key = "9938d82c228558cdebf0bdbc06ab6dab6a4f8988"
query_link = "https://api.checkr.com/v1/subscriptions/4722c07dd9a10c3985ae432a"
r = requests.delete(query_link, auth=(key, ""), data = {'candidate_id':candidate_id, 'package':'mvr_only'})
j = r.json()
print j




key = "9938d82c228558cdebf0bdbc06ab6dab6a4f8988"
sub_id = 'da8c11034ce0d0dceccba275'
query_link = "https://api.checkr.com/v1/subscriptions/"+sub_id
r = requests.delete(query_link, auth=(key,''))
j = r.json()
print j



key = "9938d82c228558cdebf0bdbc06ab6dab6a4f8988"
query_link = "https://api.checkr.com/v1/subscriptions?per_page=100&page=5&order_by=created_at&order=asc"
r = requests.get(query_link, auth=(key,''), data={'id':'6d75f58a03a2'})
j = r.json()
print j

sub_id = ''
for i in j['data']:
    sub_id = i['id']
    query_link = "https://api.checkr.com/v1/subscriptions/"+sub_id
    r = requests.delete(query_link, auth=(key,''))