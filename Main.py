import requests
import json
import sqlite3
import csv
import os

# Main
def main():
    # clear terminal window
    os.system("clear")
    buildDB()
    # write the header in the csv
    csvFile = open('output.csv', 'w')
    writer = csv.writer(csvFile)
    writer.writerow(['ID', 'Name', 'Capital', 'Population', 'Flag', 'Callingcode', 'Currency', 'Average Exchange Rate'])


    while True:
        searchCountry = raw_input("Which country do you want to search: ")
        res = searchDataForCountry(searchCountry)
        if res == None:
            quit = raw_input("Do you want to quit ? y/n: ")
            if quit == "y":
                break
            else:
                continue
        else:
            currCode = getCurrCode(searchCountry)
            currList = getExchange(currCode)
            if currList == None:
                quit = raw_input("Do you want to quit ? y/n: ")
                if quit == "y":
                    break
                else:
                    continue
            else:
                avg = calculateAvgExchange(currList)
                writeDataTocsv(writer, res, avg)
                print "Average exchange rate last month for " + searchCountry + " is: ", avg
        quit = raw_input("Do you want to quit ? y/n: ")
        if quit == "y":
            break
        else:
            continue

# build the Database
def buildDB():
    # specify the url
    url = "https://restcountries.eu/rest/v2/all?fields=callingCodes;capital;population;name;flag"
    urlCallingCodes = "https://restcountries.eu/rest/v2/all?fields=callingCodes"
    urlCurrency = "https://restcountries.eu/rest/v2/all?fields=currencies"
    # deserialise the json data
    # parsedData is now a list of python dictionaries
    parsedDataCountries = json.loads(requests.get(url).text)
    parsedDataCallingCodes = json.loads(requests.get(urlCallingCodes).text)
    parsedDataCurrencies = json.loads(requests.get(urlCurrency).text)

    # reset tables on run
    dropTables = "DROP TABLE IF EXISTS country;" \
                 "DROP TABLE IF EXISTS callingcode;" \
                 "DROP TABLE IF EXISTS currency;" \
                 "DROP TABLE IF EXISTS countries;"
    cursor.executescript(dropTables)

    # create the first table
    countryTable = "CREATE TABLE country(id INTEGER PRIMARY KEY, name TEXT, capital TEXT, population INTEGER, flag TEXT)"
    cursor.execute(countryTable)

    # fill the db with the required data
    countryInsert = "INSERT INTO country(id, name, capital, population, flag) " \
                    "VALUES(NULL,:name, :capital, :population, :flag)"
    cursor.executemany(countryInsert, parsedDataCountries)

    # found this code on the web
    class DictQuery(dict):
        def get(self, path, default=None):
            keys = path.split("/")
            val = None

            for key in keys:
                if val:
                    if isinstance(val, list):
                        val = [v.get(key, default) if v else None for v in val]
                    else:
                        val = val.get(key, default)
                else:
                    val = dict.get(self, key, default)

                if not val:
                    break;

            return val

    # get callingcodes and put them in a table
    callingCodeTable = "CREATE TABLE callingcode(id INTEGER PRIMARY KEY, callingCode TEXT)"
    cursor.execute(callingCodeTable)
    for codes in parsedDataCallingCodes:
        callingCodeInsert = "INSERT INTO callingcode(id, callingCode) VALUES(NULL, ?)"
        callingCode = DictQuery(codes).get("callingCodes")[0]
        cursor.execute(callingCodeInsert, (callingCode,))

    # get currencycodes and put them in a table
    currencyTable = "CREATE TABLE currency(id INTEGER PRIMARY KEY, currencyCode TEXT)"
    cursor.execute(currencyTable)
    for currency in parsedDataCurrencies:
        currencyInsert = "INSERT INTO currency(id, currencyCode) VALUES(NULL, ?)"
        currencyCode = DictQuery(currency).get("currencies/code")[0]
        cursor.execute(currencyInsert, (currencyCode,))

    # create final table
    finalTable = "CREATE TABLE countries(id INTEGER PRIMARY KEY, name TEXT, capital TEXT, population INTEGER, " \
                 "flag TEXT, callingCode TEXT, currencyCode TEXT)"
    cursor.execute(finalTable)

    # join all country properties into final table
    join = "INSERT INTO countries(id, name, capital, population, flag, callingCode, currencyCode) " \
           "SELECT country.id, " \
           "country.name, " \
           "country.capital, " \
           "country.population, " \
           "country.flag, " \
           "callingcode.callingCode, " \
           "currency.currencyCode " \
           "FROM country " \
           "LEFT JOIN callingcode " \
           "ON country.id = callingcode.id " \
           "LEFT JOIN currency " \
           "ON country.id = currency.id"
    cursor.execute(join)

    # commit the db changes
    db.commit()

# Retrieve the infomation of a country
# country name must match db name
def searchDataForCountry(country):
    query = """SELECT * FROM countries WHERE countries.name = "%s" ;""" % country
    cursor.execute(query)
    result = cursor.fetchall()
    if result == []:
        print "The country you searched for is incorrect."
    else:
        return result

# Retrieve the currency code of the country
def getCurrCode(country):
    query = """SELECT countries.currencyCode FROM countries WHERE countries.name = "%s" ;""" % country
    cursor.execute(query)
    result = cursor.fetchone()
    return result

# Retrieve the exchange rates of the country for november
def getExchange(currencyCode):
    exchangeList = []
    for i in range(1,31):
        urlExchange = "https://api.fixer.io/2017-11-" + str(i).zfill(2)
        parsedDataExchange = json.loads(requests.get(urlExchange).text)
        try:
            exchangeList.append(parsedDataExchange["rates"]["%s" % currencyCode])
            return exchangeList
        except KeyError:
            print "This currency is not supported"
            break

def calculateAvgExchange(exchangeList):
    return sum(exchangeList)/len(exchangeList)

def writeDataTocsv(writer,countryProp,exchangeRate):
    exchngTUP = (exchangeRate,)
    finalTup = countryProp[0] + exchngTUP
    goodStr = [s.encode('utf8') if type(s) is unicode else s for s in finalTup]
    print goodStr
    writer.writerow(goodStr)


# create the database in memory
db = sqlite3.connect(':memory:')
# db cursor object
cursor = db.cursor()
main()
