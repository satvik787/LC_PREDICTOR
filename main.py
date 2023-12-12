from fastapi import FastAPI
from pydantic import BaseModel
import uvicorn
import ranking
from threading import Thread
from json import dump
from fastapi.middleware.cors import CORSMiddleware
   

class OneRank(BaseModel):
    contestId: str
    userId: str

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

th = Thread(target=ranking.background)
th.start()

@app.get("/")
def root():
    return {"total":ranking.DB.getContestsCount(),"contests":ranking.DB.getLatestContests()}


@app.get("/ranks/{contestId}")
def getRanks(contestId,page = 0):
    return {"total":ranking.DB.getRankCount(contestId),"ranks":ranking.DB.getContestRanksUser(contestId,(int(page)))}

@app.get("/ranks/{contestId}/{userName}")
def getUserRank(contestId,userName):
    data = ranking.DB.getUserRank(userName,contestId)
    if data:
        return {"total":1,"ranks":[data]}
    else:
        return {"total":0,"ranks":[]}

@app.get("/rank")
def getRank(rank:OneRank):
    return rank

uvicorn.run(app,host="0.0.0.0",port=5000)