from collections import deque
from threading import Thread,Event
from databaseAccess import DAO

class writeToDatabase:

    LOG = "LOG_MESSAGE"
    STOP = "STOP"
    def __init__(self):
        self.q = deque()
        self.thread = None
        self.dao:DAO = DAO() 
        self.event = Event()
        self.__kill = False

    def __exector(self):
        while True:
            if self.__kill:break
            if len(self.q) > 0:
                func,args = self.q.popleft()
                if func == DAO.INSERT_USER:
                    self.dao.insertUser(*args)
                elif func == DAO.INSERT_CONTEST:
                    self.dao.insertContest(*args)
                elif func == DAO.INSERT_RANK:
                    self.dao.insertRank(*args)
                elif func == DAO.UPDATE_USER_CONTEST_CNT:
                    self.dao.updateUserContestCnt(*args)
                elif func == DAO.UPDATE_USER_RATING:
                    self.dao.updateUserRating(*args)
                elif func == DAO.UPDATE_RANK_DELTA:
                    self.dao.updateRankDeleta(*args)
                elif func == DAO.UPDATE_CONTEST_STATUS:
                    self.dao.updateContestStatus(*args)
                elif func == self.LOG:
                    print(" ".join([str(x) for x in args]))
                elif func == self.STOP:
                    print("DB WRITE STOP")
                    break
            else:
                self.event.wait(50000)

    def submit(self,func,args=()):
        try:
            self.q.append((func,args))
        except:
            breakpoint()
        if self.thread is None or not self.thread.is_alive():
            self.thread = Thread(target=self.__exector)
            self.thread.start()
        self.event.set()


    def kill(self):
        self.__kill = True