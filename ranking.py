import sys
sys.path.append("./database")

import requests
from models.Rank import Rank
import predictor
from database.databaseAccess  import DAO
from database.write import writeToDatabase
from threading import Thread,Lock
from uuid import uuid4
import calendar,datetime
import time

THREAD_CNT = 50

dbWrite = writeToDatabase()
DB = DAO.init()
lock = Lock()


def requestUserData(userName):
    res = requests.get("""https://leetcode.com/graphql?query=query
{""" +   
      f"userContestRanking(username:\"{userName}\")" 
      """{
        attendedContestsCount
        rating
        globalRanking
        totalParticipants
        topPercentage    
      }"""+
      f"userContestRankingHistory(username: \"{userName}\")"+
      """{
        attended
        trendDirection
        problemsSolved
        totalProblems
        finishTimeInSeconds
        rating
        ranking
        contest 
        {
          title
          startTime
        }
      }
}""")
    try:
        jsonData = res.json()
        if jsonData is not None:
            if jsonData.get("errors") is None:
                data = jsonData.get("data")
                if data is not None: 
                    ranking = data.get("userContestRanking")
                    if ranking is None:
                        return {"attendedContestsCount": 0,"rating": 1500}
                    return ranking
                else:
                    print("USER NOT FOUND")
                    return None
            else:
                print("USER NOT FOUND")
                return None
        else:
            print("USER NOT FOUND")
            return None
    except Exception as e:
        print("ERR",e)
        return None

def requestContestRankingBatch(start,end,title,contestID,contestTime,failed):
    try:
        dbRead = DAO()
        for page in range(start,end):
            url = f"https://leetcode.com/contest/api/ranking/{title}/?pagination={page}&region=global"
            res = requests.get(url)
            data = res.json()
            if len(data.get("submissions")[0]) == 0:break
            ranksObj = data.get("total_rank")
            for ind in range(len(ranksObj)):
                i = ranksObj[ind]
                userName = i.get("username")
                rank = i.get("rank")
                score = i.get("score")
                finishTime = i.get("finish_time")
                if score == 0 and finishTime == contestTime:continue
                userRow = dbRead.getUser(userName)
                if userRow:
                    dbWrite.submit(DAO.INSERT_RANK,(rank,userRow[0],contestID,userRow[2],False)) 
                else:
                    userId = str(uuid4())
                    userData = requestUserData(userName)
                    if userData:
                        dbWrite.submit(DAO.INSERT_USER,(userId,userName,userData["rating"]))
                        dbWrite.submit(DAO.INSERT_RANK,(rank,userId,contestID,userData["rating"],userData["attendedContestsCount"] == 0))
    except Exception:        
        print("request Failed")
        lock.acquire()
        failed.append((page,end,ind,title,contestID,contestTime))
        lock.release()
    finally:
        dbRead.cursor.close()
        dbRead.connection.close()  

def requestContestRanking(title,contestId,contestTime):
    res = requests.get(f"https://leetcode.com/contest/api/ranking/{title}/?region=global")
    data = res.json()
    numUser = data['user_num']
    totalPages = (numUser//25) + (1 if numUser % 25 != 0 else 0)
    perThread = totalPages // THREAD_CNT
    start = 1
    failed = []
    threads = []
    for i in range(THREAD_CNT):
        end = (start + perThread) + (1 if i == THREAD_CNT - 1 else 0)
        thread = Thread(target=requestContestRankingBatch,args=(start,end,title,contestId,contestTime,failed))
        threads.append(thread)
        thread.start()
        start += perThread
    for i in threads:
        i.join()
    return failed

def requestFailedRanking(failed,newFailed):
    try:
        dbRead = DAO()
        start,end,ind,title,contestID,contestTime = failed
        for page in range(start,end):
            url = f"https://leetcode.com/contest/api/ranking/{title}/?pagination={page}&region=global"
            res = requests.get(url)
            data = res.json()
            if len(data.get("submissions")[0]) == 0:break
            ranksObj = data.get("total_rank")
            l = 0 if page != start else ind
            for i in range(l,len(ranksObj)):
                userName = ranksObj[i].get("username")
                rank = ranksObj[i].get("rank")
                score = ranksObj[i].get("score")
                finishTime = ranksObj[i].get("finish_time")
                if score == 0 and finishTime == contestTime:continue
                userRow = dbRead.getUser(userName)
                if userRow:
                    dbWrite.submit(DAO.INSERT_RANK,(rank,userRow[0],contestID,userRow[2],False)) 
                else:
                    userId = str(uuid4())
                    userData = requestUserData(userName)
                    if userData:
                        dbWrite.submit(DAO.INSERT_USER,(userId,userName,userData["rating"]))
                        dbWrite.submit(DAO.INSERT_RANK,(rank,userId,contestID,userData["rating"],userData["attendedContestsCount"] == 0))
    except Exception:
        print("request Failed")
        lock.acquire()
        newFailed.append((page,end,i,title,contestID,contestTime))
        lock.release()
    finally:
        dbRead.cursor.close()
        dbRead.connection.close()

def resolveFailedJobs(failedJobs):
    newfailedJobs = []
    threads = []
    for i in failedJobs:
        thread = Thread(target=requestFailedRanking,args=(i,newfailedJobs))
        threads.append(thread)
        thread.start()
    for i in threads:
        i.join()
    return newfailedJobs



def requestContests(contestId):
    res = requests.get("https://leetcode.com/graphql?query=query%20{%20allContests%20{%20containsPremium%20title%20titleSlug%20startTime%20duration%20originStartTime%20isVirtual%20}%20}")
    data = res.json().get("data").get("allContests")
    lastContest = DB.getLatestContestTime()
    date = datetime.datetime.utcnow()
    utc_time = calendar.timegm(date.utctimetuple())
    if lastContest is None:lastContest = 1698546600 
    for i in data:
        if lastContest < i.get("startTime") < utc_time:
            dbWrite.submit(DAO.INSERT_CONTEST,(contestId,i.get("title"),i.get("titleSlug"),i.get("startTime"))) 
            return requestContestRanking(i.get("titleSlug"),contestId,i.get("startTime"))
    return []

def predictRating():
    contest = DB.getLatestContest()
    res = DB.getContestRanks(contest[3])
    ranks = []
    add = 0
    if len(res) > 0 and res[0][1] == 0:add = 1
    for i in res:
        ranks.append(Rank(i[4],i[1] + add,i[5],i[6]))
    try:
        predictor.predict(ranks)
        for i in ranks:
            dbWrite.submit(DAO.UPDATE_RANK_DELTA,(contest[3],i.userId,i.delta))
        dbWrite.submit(DAO.UPDATE_CONTEST_STATUS,(contest[3],2))
    except Exception as e:
        print(f"PREDICT FAILED {e}")

        
def background():
    while True:
        date = datetime.datetime.today()
        day = date.weekday()
        if day == 5 or day == 6:
            print("FETCHING RANKS")
            contestId = str(uuid4())
            failed = requestContests(contestId)
            attempt = 5
            waitTime = 1800
            while len(failed) > 0 and attempt > 0:
                time.sleep(waitTime)
                print("FETCHING FAILED RANKS")
                failed = resolveFailedJobs(failed)
                attempt -= 1
                waitTime += 600
            if len(failed) == 0:
                DB.updateContestStatus(contestId,1)
                predictRating()
                DB.updateContestStatus(contestId,2)
                DB.updatePredictedTime(contestId,int(time.time()))
        time.sleep(18000)

