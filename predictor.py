from math import sqrt
from models.Rank import Rank
import time

def meanWinningPercentage(r_a:float,r_b:float)->float:
    try:
        return 1/(1+pow(10,(r_b - r_a)/400))
    except OverflowError:
        return 0

def getExpectedRank(ranks:list[Rank],curUser:float)->float:
    s = 0
    for i in ranks:
        if i.currentRating != -1:
            s += meanWinningPercentage(i.currentRating,curUser)
    return s

def getRating(ranks:list[Rank],GMean:float):
    l = 1
    r = 1e6
    mid = 0
    seed = 0 
    while(r-l>0.1):
        mid = l + (r-l)/2
        seed = 1 + getExpectedRank(ranks,mid)
        if seed > GMean:
            l = mid
        else:
            r = mid
    return mid

def predict(ranks:list[Rank]):
    start = time.time()
    for i in range(len(ranks)):
        if ranks[i].currentRating == -1:
            ranks[i].predictedRating = -1
        else:
            expectedRank = 0.5 + getExpectedRank(ranks,ranks[i].currentRating)
            GMean = sqrt(expectedRank * ranks[i].rank)
            expectedRating = getRating(ranks,GMean)
            delta = expectedRating - ranks[i].currentRating
            if ranks[i].isFirstContest:delta *= 0.5
            else:delta = (delta*2)/9
            ranks[i].delta = delta
            ranks[i].predictedRating = ranks[i].currentRating + delta
    end = time.time()
    print(f"Execution Time {(end-start)*1000}")
