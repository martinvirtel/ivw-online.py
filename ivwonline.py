import pandas as pd
import requests
import logging,sys
import re
from io import StringIO


logging.basicConfig(stream=sys.stderr,level=logging.DEBUG)
logger=logging.getLogger(__name__)







headers={ 'Referer' : 'http://ausweisung.ivw-online.de/index.php?i=10&mz_szm={year}{month}&hinweiscsv=1&pis=0&az_filter=0&kat1={kat1}&kat2=0&kat3=0&kat4=0&kat5=0&kat6={kat7}&kat7=40&kat8=0&sort=&suche=',
          'Cookie'    : 'ivwausweisung=1'
        }

url=    'http://ausweisung.ivw-online.de/index.php?csv=1&i=10&mz_szm={year}{month}&az_filter=0&kat1={kat1}&kat2=0&kat3=0&kat4=0&kat5=0&kat6=0&kat7={kat7}&kat8=0&sort=&suche=&pis=0'

def int_or_NaN(a) :
    try :
        return int(a.replace(".",""))
    except ValueError as e :
        # logger.debug("Value %s: %s -> NaN" % (a,e))
        return float("NaN")

def site_or_None(s) :
    alls=" ".join([repr(a) for a in s])
    for r in (re.compile(r"http://(?P<hostname>[^/\)]+)"),
              re.compile(r"\b(?P<hostname>([a-zA-Z\-\_]+\.)+[a-zA-Z]{2,3})\b")) :
        m=r.search(alls)
        if m :
            m=m.groupdict()["hostname"].lower()
            #logger.debug("Hostname {alls} -> {m}".format(**locals()))
            return m
    return False

def get_ivw(year,month,kat7="40",kat1="9") :
    """
    get Pandas dataframe
    year "2015"
    month "10"

    kat7 ... 40 = "news"

    kat1 ... 9 ... "german"

    """
    param=locals().copy()
    csvstream=requests.get(url.format(**param),headers=dict([(key,val.format(**param)) for (key,val) in headers.items()]))
    databuffer=StringIO()
    charset="latin-1"  # no hint of charset in HTTP headers
    databuffer.write("".join([a.decode(charset) for a in csvstream.iter_content()]))
    databuffer.seek(0)
    df=pd.read_csv(databuffer,sep=";",quotechar='"',header=4,index_col=[0,1,2],thousands=".")
    # correct numbers formatted with . as 000-separator
    # parameter thousands = "." does not work for every column
    for column in df.columns :
        col=df[column]
        if type(col.iloc[0])==str  and re.match(r"^\d+\.?\d*$",col.iloc[0]) :
            logger.debug("Converting to int: column {column}".format(**locals()))
            df[column]=df[column].apply(int_or_NaN)
    # extract hostname from index
    df["url"]=df.index.to_series().apply(lambda a: site_or_None(a))
    return df


if __name__=="__main__" :
    class Config :
        month="09"
        year="2015"
    p=get_ivw(Config.year,Config.month)
    logger.debug("news: %s" % len(p))
    p=get_ivw(Config.year,Config.month,"0")
    logger.debug("all german: %s" % len(p))






# curl 'http://ausweisung.ivw-online.de/index.php?csv=1&i=10&mz_szm=201508&az_filter=0&kat1=0&kat2=0&kat3=0&kat4=0&kat5=0&kat6=0&kat7=40&kat8=0&sort=&suche=&pis=0' -H 'Pragma: no-cache' -H 'Accept-Encoding: gzip, deflate, sdch' -H 'Accept-Language: de,en-US;q=0.8,en;q=0.6,es;q=0.4' -H 'Upgrade-Insecure-Requests: 1' -H 'User-Agent: Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/46.0.2490.80 Safari/537.36' -H 'Accept: text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8' -H 'Referer: http://ausweisung.ivw-online.de/index.php?i=10&mz_szm=201509&hinweiscsv=1&pis=0&az_filter=0&kat1=0&kat2=0&kat3=0&kat4=0&kat5=0&kat6=0&kat7=40&kat8=0&sort=&suche=' -H 'Cookie: ivwausweisung=1' -H 'Connection: keep-alive' -H 'Cache-Control: no-cache' --compressed >szcurl.csv
# curl 'http://ausweisung.ivw-online.de/index.php?csv=1&i=10&mz_szm=201509&az_filter=0&kat1=9&kat2=0&kat3=0&kat4=0&kat5=0&kat6=0&kat7=0&kat8=0&sort=&suche=&pis=0' -H 'Pragma: no-cache' -H 'Accept-Encoding: gzip, deflate, sdch' -H 'Accept-Language: de,en-US;q=0.8,en;q=0.6,es;q=0.4' -H 'Upgrade-Insecure-Requests: 1' -H 'User-Agent: Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/46.0.2490.80 Safari/537.36' -H 'Accept:
# text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8' -H 'Referer: http://ausweisung.ivw-online.de/index.php?i=10&mz_szm=201509&hinweiscsv=1&pis=0&az_filter=0&kat1=9&kat2=0&kat3=0&kat4=0&kat5=0&kat6=0&kat7=0&kat8=0&sort=&suche=' -H 'Cookie: ivwausweisung=1' -H 'Connection: keep-alive' -H 'Cache-Control: no-cache' --compressed
