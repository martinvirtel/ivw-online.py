import logging,sys
import pandas as pd
from ivwonline import get_ivw



logging.basicConfig(stream=sys.stderr,level=logging.DEBUG)
logger=logging.getLogger(__name__)


class Config :

    month="09"
    year="2015"
    category="40"
    baseline="test-publishers.csv"


    annotated_baseline="result/test-publishers-annotated.csv"

    missing_ivw="result/test-publishers-missing.csv"



def find_match(base,sources) :
    if type(base)==str  :
        for (s,v) in sources.items() :
            if base.find(s)>-1 :
                return v
    return False

def find_visits(url,df) :
    if url :
        return df.ix[url]["Kat-Visits"]
    return float("NaN")


if __name__=="__main__" :
    ivw=get_ivw(Config.year,Config.month,Config.category)
    by_url=ivw.groupby("url").aggregate({ "Kat-Visits": "max" })
    sources=dict([(a.replace("www",""),a) for a in ivw["url"] if a])
    baseline=pd.read_csv(Config.baseline)
    baseline["ivw"]=baseline["url"].apply(lambda a: find_match(a,sources))
    baseline["ivw_visits"]=baseline["ivw"].apply(lambda a: find_visits(a,by_url))
    baseline.to_csv(Config.annotated_baseline)

    websites=ivw[ivw.index.to_series().apply(lambda a: "Online" in a[2])]
    found=set([a  for a in baseline["ivw"]])
    not_found=websites[websites["url"].apply(lambda a: a not in found)]
    not_found.to_csv(Config.missing_ivw)


