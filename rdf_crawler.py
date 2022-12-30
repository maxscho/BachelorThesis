### Import dependencies
import datetime
import sys
import os
import random
from stem import Signal
from stem.control import Controller

import pandas as pd
import requests
import json
from rich.console import Console
from rich.prompt import Prompt
from rich import inspect

import time
from rich.progress import track

import hashlib

import requests
import concurrent
from concurrent.futures import ThreadPoolExecutor

import psutil

console = Console()

def get_tor_session():
    torSession = requests.session()
    # Tor uses the 9050 port as the default socks port
    torSession.proxies = {'http':  'socks5://127.0.0.1:9050',
                       'https': 'socks5://127.0.0.1:9050'}
    return torSession

# signal TOR for a new connection
def renew_connection():
    with Controller.from_port(port = 9051) as controller:
        controller.authenticate(password="passMeTheCake42!")
        controller.signal(Signal.NEWNYM)

#Change ip
#renew_connection()

print("Origin: " + str(json.loads(requests.get("http://httpbin.org/ip").text)['origin']))

torSession = get_tor_session()
print("Masked: " + str(json.loads(torSession.get("http://httpbin.org/ip").text)['origin']))

##########################################################################################

def getData(smwTable):
    ## create output folders
    if not os.path.exists('results'):
        os.makedirs('results')
    if not os.path.exists('results/pages'):
        os.makedirs('results/pages')

    threads = 100

    #create hashing function
    def hashStr(text):
        return str(hashlib.md5( (str(text)).encode('utf-8') ).hexdigest())

    #check for any previous progress to continue with
    if os.path.exists('results/pages/continueParamStore.json'):
        with open('results/pages/continueParamStore.json', 'r', encoding='utf-8') as fcc_file:
            fcc_data = json.load(fcc_file)
            continueParamStore = fcc_data
            #jsonNamingCounter = fcc_data[0]
        console.print('Could find previous progress (results folder). Delete the results folder if you want to start from scratch. I will continue from where we left off:', style="green")
        inspect(continueParamStore)
    #otherwise start from scratch
    else:
        continueParamStore = {}

        for index, row in smwTable.iterrows():
            hashedFolderName = hashStr(str(row['Has API URL']))
            if not os.path.exists('results/pages/' + hashedFolderName):
                os.makedirs('results/pages/' + hashedFolderName)
            continueParamStore[str(row['Has API URL'])] = ["","", hashedFolderName, 0, [0,0]]

        #jsonNamingCounter = 0

    #get all articles of a page
    def getArticles(param):

        if not os.path.exists('results/rdf'):
            os.makedirs('results/rdf')

        try:
            currentPage = torSession.get(f'{param[0]}?action=query&format=json&list=allpages&aplimit=500&apcontinue={param[1][0]}&continue={param[1][1]}', timeout=10).text
            return [param, currentPage]
        except Exception as e:
            return [param, e]

    while len(continueParamStore.items()) > 0 and 1 == 0:

        inspect(continueParamStore)
        #inspect(jsonNamingCounter)
        with open('results/pages/continueParamStore.json', "w", encoding='utf-8') as outfile:
            outfile.write(json.dumps(continueParamStore, ensure_ascii=False))

        with ThreadPoolExecutor(max_workers=threads) as executor:
            continueParamList = continueParamStore.items()
            #inspect(continueParamList)
            future2URL = {executor.submit(getArticles, param) for param in continueParamList}
            
            for future in concurrent.futures.as_completed(future2URL): 
                #lastItem = ""
                try:
                    r = future.result()
                    #inspect(r)
                    continueParamStore[r[0][0]]
                    [4][0] = continueParamStore[r[0][0]][4][0] + 1 #iterations
                    #lastItem = r[0][0]
                    #inspect(r)
                    hashedFolderName = r[0][1][2]
                    currentPage = r[1]
                    #inspect(currentPage)
                    errorTrigger = 0
                    if type(currentPage) == requests.exceptions.ConnectTimeout:
                        errorTrigger = 1
                        #del continueParamStore[r[0][0]]
                        continueParamStore[r[0][0]][4][1] = continueParamStore[r[0][0]][4][1] + 1
                        console.print('Timeout again:')
                        #inspect(continueParamStore[r[0][0]][4][1] / continueParamStore[r[0][0]][4][0])
                        if continueParamStore[r[0][0]][4][0] >= 10 and (continueParamStore[r[0][0]][4][1] / continueParamStore[r[0][0]][4][0]) >= 0.1:
                            del continueParamStore[r[0][0]]
                        raise Exception(currentPage)

                    try:
                        jsonResult = json.loads(currentPage)
                        #inspect(jsonResult)
                    except Exception as e1:
                        errorTrigger = 1
                        continueParamStore[r[0][0]][4][1] = continueParamStore[r[0][0]][4][1] + 1
                        console.print('JSON doesn\'t load:')
                        #inspect(continueParamStore[r[0][0]][4][1] / continueParamStore[r[0][0]][4][0])
                        if continueParamStore[r[0][0]][4][0] >= 10 and (continueParamStore[r[0][0]][4][1] / continueParamStore[r[0][0]][4][0]) >= 0.1:
                            del continueParamStore[r[0][0]]
                    #      continueParamStore[index] = [APIURL,str(jsonResult["continue"]["apcontinue"]),str(jsonResult["continue"]["continue"]),hashedFolderName, index, jsonNamingCounter + 1]

                    with open('results/pages/' + hashedFolderName + '/' + str(continueParamStore[r[0][0]][3]) + ".json", "w", encoding='utf-8') as outfile:
                        outfile.write(json.dumps(jsonResult, ensure_ascii=False))
                    continueParamStore[r[0][0]][3] = continueParamStore[r[0][0]][3] + 1 #jsonNamingCounter

                    if "continue" in jsonResult and errorTrigger == 0:
                        #if continueParamStore[r[0][0]][0] == jsonResult["continue"]["apcontinue"] and continueParamStore[r[0][0]][1] == jsonResult["continue"]["continue"]:
                        #    del continueParamStore[r[0][0]]
                        #else:
                        #inspect(continueParamStore[r[0][0]][0])
                        continueParamStore[r[0][0]][0] = jsonResult["continue"]["apcontinue"]
                        #inspect(continueParamStore[r[0][0]][0])
                        #inspect(continueParamStore[r[0][0]][1])
                        continueParamStore[r[0][0]][1] = jsonResult["continue"]["continue"]
                        #inspect(continueParamStore[r[0][0]][1])
                        #inspect(continueParamStore[r[0][0]][3])
                        continueParamStore[r[0][0]][4][1] = 0 #Success flushes all errors
                        #inspect(continueParamStore[r[0][0]][3])
                        #continueParamStore[r[0][0]] = [jsonResult["continue"]["apcontinue"], jsonResult["continue"]["continue"], r[0][1][2]]
                    elif errorTrigger == 0:
                        del continueParamStore[r[0][0]]


                    #print(str(data))
                    print('CPU %: ' + str(psutil.cpu_percent()) + ' MEM %: ' + str(psutil.virtual_memory().percent))
                except Exception as e:
                    print('Looks like something went wrong:', e)
                    inspect(e)
                    #if str(e).startswith('SOCKSHTTPConnectionPool'):
                    print(type(e))
                    #inspect(continueParamStore)
                    #del continueParamStore[r[0][0]]
                    #inspect(continueParamStore[r[0][0]])
                    #    del continueParamStore[r[0][0]]
                    #inspect(e)
                    #del continueParamStore[lastItem]


        console.print('Done with batch', style="green")
        time.sleep(0.03)
        #jsonNamingCounter = jsonNamingCounter + 1

    #count = 0
    console.print('Done with the article crawling. You should check the results.', style="green")
    console.print('Starting the RDF crawling.', style="green")

    if os.path.exists('results/rdf/continueParamStoreRDF.json'):
        with open('results/rdf/continueParamStoreRDF.json', 'r', encoding='utf-8') as fcc_file:
            fcc_data = json.load(fcc_file)
            continueParamStoreRDF = fcc_data
            #rdfNamingCounter = fcc_data[0]
        console.print('Could find previous progress with RDF crawling (results folder). Delete the results folder if you want to start from scratch. I will continue from where we left off:', style="green")
        inspect(continueParamStoreRDF)
    else:
        continueParamStoreRDF = {}

        for index, row in smwTable.iterrows():
            hashedFolderName = hashStr(str(row['Has API URL']))
            if not os.path.exists('results/rdf/' + hashedFolderName):
                os.makedirs('results/rdf/' + hashedFolderName)
            continueParamStoreRDF[str(row['Has API URL'])] = [[0,0], row['Has interwiki URL'], hashedFolderName, [0,0]]

        #rdfNamingCounter = [0,0]

    #method to extract RDF content
    def getRDF(param):
        try:
            # open json
            articlesFile = open("results/pages/" + str(param[1][2]) + "/" + str(param[1][0][0]) + ".json", encoding='utf-8')
            articlesRegisterFull = json.load(articlesFile)
            articlesRegister = articlesRegisterFull["query"]["allpages"]
            # get right id
            #inspect(articlesRegister[param[1][0][1]]['title'])
            #print(f'{param[1][1].replace("$1", "Special:ExportRDF/" + str(articlesRegister[param[1][0][1]]['title']))}')
            articleName = articlesRegister[param[1][0][1]]
            #inspect(articleName['title'])
            urlEnding = "Special:ExportRDF/" + str(articleName['title'])
            #inspect(urlEnding)
            #a = 1 + "a"
            currentPage = torSession.get(f'{str(param[1][1]).replace("$1", urlEnding)}', timeout=10).text
            return [param, currentPage]
        except Exception as e:
            return [param, e]

    # Collect error statistics
    if os.path.exists('results/rdf/errors.json'):
        with open('results/rdf/errors.json', 'r', encoding='utf-8') as err_file:
            errors = json.load(err_file)
    else:
        errors = {}

    while len(continueParamStoreRDF.items()) > 0:

        inspect(continueParamStoreRDF)
        #inspect(rdfNamingCounter)
        with open('results/rdf/continueParamStoreRDF.json', "w", encoding='utf-8') as outfile:
            outfile.write(json.dumps(continueParamStoreRDF, ensure_ascii=False ))

        with ThreadPoolExecutor(max_workers=threads) as executor:
            continueParamListRDF = continueParamStoreRDF.items()
            inspect(continueParamListRDF)
            # continueParamListRDFTimestamped = continueParamListRDF
            # for index, param in continueParamListRDFTimestamped:
            #     if param[1][3][2] >  time.time():
            #         del continueParamListRDFTimestamped[index]

            future2URL = {executor.submit(getRDF, param) for param in continueParamListRDF}
            for future in concurrent.futures.as_completed(future2URL):
                #lastItem = ""
                try:
                    r = future.result()
                    continueParamStoreRDF[r[0][0]][3][0] = continueParamStoreRDF[r[0][0]][3][0] + 1
                    #lastItem = r[0][0]
                    #inspect(r)
                    hashedFolderName = r[0][1][2]
                    currentPage = r[1]
                    #inspect(currentPage)
                    errorTrigger = 0
                    if type(currentPage) == requests.exceptions.ConnectTimeout:
                        errorTrigger = 1
                        # continueParamStoreRDF[r[0][0]][3][2] = time.time() + 60*10
                        continueParamStoreRDF[r[0][0]][3][1] = continueParamStoreRDF[r[0][0]][3][1] + 1
                        console.print('Timeout again:')
                        #inspect(continueParamStore[r[0][0]][4][1] / continueParamStore[r[0][0]][4][0])
                        if continueParamStoreRDF[r[0][0]][3][0] >= 10 and (continueParamStoreRDF[r[0][0]][3][1] / continueParamStoreRDF[r[0][0]][3][0]) >= 0.1:
                            del continueParamStoreRDF[r[0][0]]
                        raise Exception(currentPage)

                    try:
                        #inspect(currentPage)
                        console.print(currentPage)
                        if "RDF" in currentPage and not "Checking if the site connection is secure" in currentPage:
                            continueParamStoreRDF[r[0][0]][3][1] = 0
                            #a = 1
                            with open('results/rdf/' + hashedFolderName + '/' + str(continueParamStoreRDF[r[0][0]][0][0]) + '_' + str(continueParamStoreRDF[r[0][0]][0][1]) + ".rdf", "w", encoding='utf-8') as outfile:
                                outfile.write(str(currentPage))
                            if continueParamStoreRDF[r[0][0]][0][1] >= 499:
                                continueParamStoreRDF[r[0][0]][0][1] = 0
                                continueParamStoreRDF[r[0][0]][0][0] = continueParamStoreRDF[r[0][0]][0][0] + 1
                            else:
                                continueParamStoreRDF[r[0][0]][0][1] = continueParamStoreRDF[r[0][0]][0][1] + 1
                        else:
                            if "Rate Limited" in currentPage:
                                errors[r[0][0]] = "Rate Limited"
                            elif "Checking if the site connection is secure" in currentPage:
                                errors[r[0][0]] = "Checking if the site connection is secure"
                            elif "403 Forbidden" in currentPage:
                                errors[r[0][0]] = "403 Forbidden"

                            errors["overall"] = [0,0,0]

                            for err in errors.items():
                                if err[1] == "Rate Limited":
                                    errors["overall"][0] = errors["overall"][0] + 1
                                if err[1] == "Checking if the site connection is secure":
                                    errors["overall"][1] = errors["overall"][1] + 1
                                if err[1] == "403 Forbidden":
                                    errors["overall"][2] = errors["overall"][2] + 1

                            with open('results/rdf/errors.json', "w", encoding='utf-8') as outfile:
                                outfile.write(json.dumps(errors, ensure_ascii=False ))

                            console.print('NO RDF', style="red")
                            errorTrigger = 1
                            continueParamStoreRDF[r[0][0]][3][1] = continueParamStoreRDF[r[0][0]][3][1] + 1
                            console.print('Timeout again:')
                            #inspect(continueParamStore[r[0][0]][4][1] / continueParamStore[r[0][0]][4][0])
                            if continueParamStoreRDF[r[0][0]][3][0] >= 10 and (continueParamStoreRDF[r[0][0]][3][1] / continueParamStoreRDF[r[0][0]][3][0]) >= 0.1:
                                del continueParamStoreRDF[r[0][0]]
                    except Exception as e1:
                        errorTrigger = 1
                        continueParamStoreRDF[r[0][0]][3][1] = continueParamStoreRDF[r[0][0]][3][1] + 1
                        console.print(e1, style="red")
                        #inspect(continueParamStore[r[0][0]][4][1] / continueParamStore[r[0][0]][4][0])
                        if continueParamStoreRDF[r[0][0]][3][0] >= 10 and (continueParamStoreRDF[r[0][0]][3][1] / continueParamStoreRDF[r[0][0]][3][0]) >= 0.1:
                            del continueParamStoreRDF[r[0][0]]
                    #      continueParamStore[index] = [APIURL,str(jsonResult["continue"]["apcontinue"]),str(jsonResult["continue"]["continue"]),hashedFolderName, index, jsonNamingCounter + 1]

                    #with open('results/' + hashedFolderName + '/' + str(rdfNamingCounter[0]) + '_' + str(rdfNamingCounter[1]) + ".rdf", "w") as outfile:
                    #    outfile.write(str(rdfResult))

                    #print(str(data))
                    print('CPU %: ' + str(psutil.cpu_percent()) + ' MEM %: ' + str(psutil.virtual_memory().percent))
                except Exception as e:
                    print('Looks like something went wrong:', e)
                    #if str(e).startswith('SOCKSHTTPConnectionPool'):
                    print(type(e))
                    #inspect(continueParamStore)
                    #del continueParamStore[r[0][0]]
                    #inspect(continueParamStore[r[0][0]])
                    #    del continueParamStore[r[0][0]]
                    #inspect(e)
                    #del continueParamStore[lastItem]


        console.print('Done with batch', style="green")
        time.sleep(0.03)


# Seed function asking to input the SMW table
def loadSMWTable(smwTablePath):
    try:
        smwTable = pd.read_csv(smwTablePath)
        smwTable = smwTable.reset_index() # make sure indexes pair with number of rows
        if 'Has API URL' in smwTable.columns and 'Has interwiki URL' in smwTable.columns:
            console.print('This is a valid .csv table!\nIt presumably contains' , smwTable.shape[0] , 'SMWs.', style="green")
            #inspect(smwTable.shape, methods=True)
            #nextStep = Prompt.ask("What should we do now?", choices=["Check alive", "Crawl RDF"], default="Check alive")

            getData(smwTable)
            #getRDF(smwTable)
    except Exception as e:
        console.print('Incorrect input. Specify the correct path to a .csv file containing SMWs\' data. (With \'Has API URL\' and \'Has interwiki URL\' columns.)', style="red")
        loadSMWTable()

# Start the programm:
loadSMWTable('a.csv')