class Rank:
    def __init__(self,userId:str,rank:int,curRating:float,isFirstContest:bool) -> None:
        self.userId = userId
        self.rank = rank
        self.delta = 0 
        self.currentRating = curRating
        self.predictedRating = -1
        self.isFirstContest = isFirstContest