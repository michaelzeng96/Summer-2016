import httplib2
import requests
from BeautifulSoup import BeautifulSoup, SoupStrainer
import pandas as pd
import collections 


#information about states, cities, and communities 
states = ['California','Texas','Colorado','Illinois','Georgia',
'Oregon','Florida','New-York','Arizona','Washington','Tennessee','Pennsylvania','Massachusetts','North-Carolina']
allCitiesLinks = []
allCommunitiesLinks = []
my_states = []
community_names = []
addresses = []

def parseAllCities(state):
    url = 'https://www.caring.com/local/independent-living-in-'+state
    #http = httplib2.Http()
    #status, response = http.request(url)
    r = requests.get(url, timeout = 5)
    soup = BeautifulSoup(r.text)
    #print 'Fetching cities from '+url+'...'
    div = soup.findAll('article', {'class':'container'})
    
    [x.extract() for x in div[0].findAll('a',{'itemprop':'url'})]
    [x.extract() for x in div[0].findAll('section',{'id':'categories'})]
    [x.extract() for x in div[0].findAll('div',{'class':'clearfix'})]
    [x.extract() for x in div[0].findAll('div',{'class':'blurb'})]
    
    x = div[0].findAll('a')
    for i in xrange(len(x)):
        allCitiesLinks.append(x[i]['href'])
    #print 'Done...\n'
    
def parseAllCommunities(city):
    url = 'https://www.caring.com'+city
    print 'Currently on '+url+'\n'
    #http = httplib2.Http()
    #status, response = http.request(url)
    r = requests.get(url, timeout = 5)
    soup = BeautifulSoup(r.text)
    #soup =  BeautifulSoup(response)
    div = soup.findAll('div', {'class' : 'col-md-8'})
    buttons = div[1].findAll('div', {'class':'col-sm-3 col-xs-12'})
    for i in xrange(len(buttons)):
        a = buttons[i].find('a')
        allCommunitiesLinks.append(a['href'])
        
    #scrape next pages
    page_count = soup.find('ul',{'class':'pager'})
    if page_count is None:
        return
    else:
        count = page_count.findAll('li', {'class':'page'})
        for i in xrange(2, len(count)+2):
            url='https://www.caring.com'+count[i-2].find('a')['href']
            print 'Currently on '+url+'\n'
            #http = httplib2.Http()
            r = requests.get(url, timeout = 5)
            soup = BeautifulSoup(r.text)
            div = soup.findAll('div', {'class' : 'col-md-8'})
            buttons = div[1].findAll('div', {'class':'col-sm-3 col-xs-12'})
            for i in xrange(len(buttons)):
                a = buttons[i].find('a')
                allCommunitiesLinks.append(a['href'])
   
    
def parseInfoFromCommunity(communityLink):
    #connect to website
    #http = httplib2.Http()
    #status, response = http.request(communityLink)
    #soup = BeautifulSoup(response)
    r = requests.get(communityLink, timeout = 5)
    soup = BeautifulSoup(r.text)
    #scrape name
    div = soup.findAll('div', {'itemprop':'name'})
    if div is None:
        community_names.append('null')
        return
    else:
        community_names.append(div[0].text)
        print 'Scraped Name for '+communityLink+'\n'
    #scrape address       
    div = soup.findAll('div', {'itemprop':'address'})
    if div is None:
        addresses.append('null')
        my_states.append('null')
        return
    else:
        street_address = soup.findAll('div', {'class':'street-address'})[0].text
        locality = soup.findAll('div', {'class':'locality'})[0].text
        region = soup.findAll('div', {'class':'region'})[0].text
        postal_code = soup.findAll('div', {'class':'postal-code'})[0].text
        address = street_address+' '+locality+' '+region+' '+postal_code
        addresses.append(address)
        my_states.append(region)
        print 'Scraped all address details for '+communityLink+'\n'

    
#grab all links for cities       
for state in states:
    parseAllCities(state)
    
#grab all community links for each city
for city in allCitiesLinks:
    parseAllCommunities(city)

#grab all info from each community
for communityLink in allCommunitiesLinks:
    parseInfoFromCommunity(communityLink)

#create dataframe
df1 = pd.DataFrame()
df = pd.DataFrame()
newStates = []
newNames = []
newAddresses = []

#remove duplicates
for i in my_states:
    if i not in newStates:
        newStates.append(i)
        
for i in community_names:
    if i not in newNames:
        newNames.append(i)
        
for i in addresses:
    if i not in newAddresses:
        newAddresses.append(i)
        
df1['Address'] = newAddresses
df['State'] = my_states
df['Community_Name'] = community_names
df['Address'] = addresses
df.to_csv('all_duplicate_data.csv.csv', encoding = 'utf-8')
df1.to_csv('unique_addresses_data.csv', encoding = 'utf-8')