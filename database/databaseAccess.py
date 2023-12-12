from threading import Lock
import mysql.connector
class DAO:
    obj = None
    INSERT_USER = 0
    INSERT_CONTEST = 1
    INSERT_RANK = 2
    UPDATE_USER_CONTEST_CNT = 3
    UPDATE_USER_RATING = 4
    UPDATE_RANK_DELTA = 5
    UPDATE_CONTEST_STATUS = 6

    @classmethod
    def init(cls):
        if cls.obj is None:
            cls.obj = DAO()
        return cls.obj
    
    def __init__(self):
        self.connection = mysql.connector.connect(
            host="localhost",
            user="root",
            password="root",
            database="lc_predictor"
        )
        self.cursor = self.connection.cursor(buffered=True)        
    
    def insertUser(self,userID,userName,rating):
        sql = "INSERT INTO users(userID,username,rating) VALUES(%s,%s,%s)"
        self.cursor.execute(sql,(userID,userName,rating))
        self.connection.commit()

    
    def insertContest(self,contestID,title,titleSlug,time):
        sql = "INSERT INTO contests(contestID,title,titleSlug,startTime) VALUES(%s,%s,%s,%s)"
        self.cursor.execute(sql,(contestID,title,titleSlug,time))
        self.connection.commit()
    
    def insertRank(self,userRank,userId,contestId,rating,firstContest):
        sql = "INSERT INTO ranks(userRank,userID,contestID,rating,firstContest) VALUES (%s,%s,%s,%s,%s)"
        self.cursor.execute(sql,(userRank,userId,contestId,rating,firstContest))
        self.connection.commit()

    def updateUserContestCnt(self,username):
        sql = "UPDATE users SET firstContest = false where username = %s"
        self.cursor.execute(sql,username)
        self.connection.commit()
    
    def updateUserRating(self,userId,rating):
        sql = "UPDATE users SET rating = %s WHERE userID = %s"
        self.cursor.execute(sql,(rating,userId))
        self.connection.commit()

    def updateRankDeleta(self,contestId,userId,delta):
        sql = "UPDATE ranks SET predictedScore = %s WHERE userID = %s and contestID = %s"
        self.cursor.execute(sql,(delta,userId,contestId))
        self.connection.commit()

    def updatePredictedTime(self,contestID,predictTime):
        sql = "UPDATE contests SET predictedTime = %s WHERE contestID = %s"
        self.cursor.execute(sql,(predictTime,contestID))
        self.connection.commit()

    def updateContestStatus(self,contestId,status):
        sql = "UPDATE contests SET status = %s WHERE contestID = %s"
        self.cursor.execute(sql,(status,contestId))
        self.connection.commit()

    def getContestsCount(self):
        sql = "SELECT count(*) FROM contests"
        self.cursor.execute(sql)
        return self.cursor.fetchone()[0]

    def getUserRank(self,userName,contestID):
        sql = "SELECT ranks.userRank,users.userName,ranks.predictedScore,ranks.rating FROM ranks JOIN users ON users.userID = ranks.userID WHERE contestID = %s AND users.userName = %s"
        self.cursor.execute(sql,(contestID,userName))
        return self.cursor.fetchone()
        

    def getRankCount(self,contestId):
        sql = "SELECT count(*) FROM ranks WHERE contestID = %s"
        self.cursor.execute(sql,(contestId,))
        return self.cursor.fetchone()[0]

    def getContestRanksUser(self,contestId,rank):
        sql = "SELECT ranks.userRank,users.userName,ranks.predictedScore,ranks.rating FROM ranks JOIN users ON users.userID = ranks.userID WHERE contestID = %s AND ranks.userRank >= %s  ORDER BY ranks.userRank limit 25"
        self.cursor.execute(sql,(contestId,rank))
        return self.cursor.fetchall()

    def getContestRanks(self,contestId):
        sql = "SELECT * FROM ranks WHERE contestID = %s ORDER BY ranks.userRank"
        self.cursor.execute(sql,(contestId,))
        return self.cursor.fetchall()        
    
    def getUser(self,username):
        sql = "SELECT * FROM users WHERE username = %s"
        self.cursor.execute(sql,(username,))
        val = self.cursor.fetchone()
        if val is None:return None
        return val

    def getLatestContests(self):
        sql = "SELECT * FROM contests ORDER BY startTime desc"
        self.cursor.execute(sql)
        val = self.cursor.fetchall()
        if val is None:return None
        return val

    def getLatestContest(self):
        sql = "SELECT * FROM contests ORDER BY startTime desc LIMIT 1"
        self.cursor.execute(sql)
        val = self.cursor.fetchone()
        if val is None:return None
        return val

    def getLatestContestTime(self):
        sql = "SELECT startTime FROM contests ORDER BY startTime desc LIMIT 1"
        self.cursor.execute(sql)
        val = self.cursor.fetchone()
        if val is None:return None
        return val[0]
    