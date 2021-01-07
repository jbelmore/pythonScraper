#import all your libraries
import csv
import re
import queue
import asyncio
import time
import concurrent.futures
import requests
from multiprocessing import Pool

#name of the csv you want to write to.
csv = 'yellowPagesScrapeAsync.csv'

#patterns for finding the phone, categories, addresses, etc in a web page.
nextPagePattern = '<a class="next ajax-page" href=".*?" data-page="'
businessPattern = '<a class="business-name" href=".*?><span>.*?</span></a>'
phonePattern = '<p class="phone".*?>.*?</p>'
categoriesPattern = '<p class="cats">.*?</p>'
emailpattern = '<a class="email-business" href=".*?>.*?</a>'
addressPattern = '<h2 class="address">.*?</h2>'
#Checks to make sure what is downloaded is an email address
EMAIL_REGEX = EMAIL_REGEX = r"""(?:[a-z0-9!#$%&'*+/=?^_`{|}~-]+(?:\.[a-z0-9!#$%&'*+/=?^_`{|}~-]+)*|"(?:[\x01-\x08\x0b\x0c\x0e-\x1f\x21\x23-\x5b\x5d-\x7f]|\\[\x01-\x09\x0b\x0c\x0e-\x7f])*")@(?:(?:[a-z0-9](?:[a-z0-9-]*[a-z0-9])?\.)+[a-z0-9](?:[a-z0-9-]*[a-z0-9])?|\[(?:(?:(2(5[0-5]|[0-4][0-9])|1[0-9][0-9]|[1-9]?[0-9]))\.){3}(?:(2(5[0-5]|[0-4][0-9])|1[0-9][0-9]|[1-9]?[0-9])|[a-z0-9-]*[a-z0-9]:(?:[\x01-\x08\x0b\x0c\x0e-\x1f\x21-\x5a\x53-\x7f]|\\[\x01-\x09\x0b\x0c\x0e-\x7f])+)\])"""

#open the CSV file that everything will be saved to.
with open(csv, 'w') as f:
    f.write("name, phone, email, address \n")

#The base URL to start
baseUrl = "https://www.yellowpages.com"


#Searh terms to search for
searchTermArray = ["windows", "landscaping", "electrician", "heating+and+cooling", "floor+covering", "concrete", "plumbing", "Home+Improvement", "painting", "Building+Contractors", "Construction+Consultants"]

#how to put together the URLs for the search
searchUrl = "/search?search_terms="
location = "&geo_location_terms="


stateArray3 = [
    "NV", "NH", "HI"
]


totalStates = int(len(stateArray3) - 1)
totalSearchTerm = int(len(searchTermArray) - 1)

#count upkeep
count = 0

#number of threads to run for multithreading
MAX_THREADS = 30

#the function to grab everything from the webpage
def get_Business_Info(htmlStuff):
    try:
        thisBiz = htmlStuff
        bizString = str(thisBiz)
        name = ''
        phone = ''
        email = ''
        address = ''

        name = str(re.sub("<.*?>", "", thisBiz))
        name = str(re.sub("&amp;", "&", name))
        name = str(re.sub(",", "", name))
        
        link = re.findall("href=[\"\'](.*?)[\"\']", bizString)
        bizUrl = str(baseUrl + link[0])
        bizPage = requests.get(bizUrl)
    
        if bizPage:
            bizhtml = bizPage.content.decode("utf-8")

        phonehtml = re.findall(phonePattern, bizhtml, re.IGNORECASE)
        if phonehtml:
            phoneRaw = re.sub("<.*?>", "", str(phonehtml[0]))
            phone = str(phoneRaw)

        emailhtml = re.findall(emailpattern, bizhtml, re.IGNORECASE)
        if emailhtml:
            emailUrl = re.search(EMAIL_REGEX, str(emailhtml[0]), re.IGNORECASE)
            emailRaw = emailUrl.group(0)
            email = str(emailRaw)
        
        addresshtml = re.findall(addressPattern, bizhtml, re.IGNORECASE)
        if addresshtml:
            addressRaw = re.sub("<.*?>", "", str(addresshtml[0]), re.IGNORECASE)
            addressRaw = re.sub("</span>", ", ", addressRaw)
            addressRaw = re.sub("</h2>", "", addressRaw)
            addressRaw = re.sub(',', "", addressRaw)
            address=str(addressRaw)
            
            #add all your business into a csv acceptable format then send to "writeToCSV" function
            totalBusiness = name + "," + phone + "," + email + "," + address
            writeToCSV(totalBusiness)

    except:
        print("Get Business Info timed out")
        pass

#the scrape term is searched with the state
def scrapeTerm(term, state):
    t0 = time.time()
    state = str(state)
    search = str(term)
    print('search term is ', search)
    print('state is ', state)
    url = str(baseUrl + searchUrl + search + location + state)
    print('url is ', url)
    content = requests.get(url)
    if content:
        html = content.content.decode("utf-8")
        totalIndex = html.find('<span>We found</span>') + len('<span>We found</span>')
        totalEndIndex = html.find('<span>results</span>')
        totalResults = int(html[totalIndex:totalEndIndex])

        #get your total pages
        pages = totalResults / 30
        pages = int(pages - 1)

        for p in range(0, pages):
            page = requests.get(url)
            if page:
                html = page.content.decode("utf-8")
                businesses = re.findall(businessPattern, html, re.IGNORECASE)
                nexturlSearch = re.search(nextPagePattern, html, re.IGNORECASE)
                if nexturlSearch:

                    nextUrl = nexturlSearch.group(0)
                    parseString = str(nextUrl)
                    linkSearch = re.findall("href=[\"\'](.*?)[\"\']", parseString)
                    link = str(linkSearch[0])
                    link = re.sub("&amp;", "&", link)
                    url = str(baseUrl + link)

                    threads = min(MAX_THREADS, len(businesses))
    
                    with concurrent.futures.ThreadPoolExecutor(max_workers=threads) as executor:
                        executor.map(get_Business_Info, businesses)
                else:
                    print("no next button")
                    pass

            else:
                print("no results for term")
                pass

    else:
        print("not web content")
        pass
    
    t1 = time.time()
    print('completed state: ', state, 'with search ', search)
    print(f"{t1-t0} seconds to download")

    #delay between data chunks to make sure you don't get blocked.
    time.sleep(35)

        
#write the information tot he csv
def writeToCSV(string):
    content = string
    with open(csv, 'a') as f:
        f.write(str(content) + "\n")


#load up your search terms to the multithread and start going
def main():
    tasks = []
    for n in stateArray3:
        print("adding state ", n)
        for t in searchTermArray:
            print("adding search term ", t)
            tasks.append(scrapeTerm(t, n))
    
    threads = min(MAX_THREADS, len(tasks))
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=threads) as executor:
        executor.map(tasks)

#main initiator of program
if __name__ == '__main__':
    t = time.perf_counter()
    main()
    t2 = time.perf_counter() - t
    print(f'total time taken: {t2:0.2f} seconds')

        




        

        
        
