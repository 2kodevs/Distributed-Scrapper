class Conit:
    """
    Consistency unit, keep a resource and some metrics.
    """
    def __init__(self, data, owners, limit=5):
        self.data = data
        self.hits = 0
        self.lives = 0
        self.limit = limit
        self.owners = owners

    
    def hit(self):
        """
        Add a hit for this resource, if limit is reached,
        then its data can be replicated by the caller.
        """
        self.hits += 1
        
        if self.hits == self.limit:
            return True
        return False


    def tryOwn(self, repLimit):
        """
        Returns True if the resource is in the tolerable replication limit.
        """
        return len(self.owners) < repLimit


    def addOwner(self, owner):
        if owner not in self.owners:
            self.owners.append(owner)

    
    def removeOwner(self, owner):
        if owner in self.owners:
            self.owners.remove(owner)

    
    def addLive(self):
        self.lives += 1
        self.hits = 0

    
    def updateData(self, data, lives=0):
        self.data = data
        self.lives = lives


    def copy(self):
        """
        Returns a copy without data.
        """
        conit = Conit(None, self.owners, self.limit)
        conit.hits = self.hits
        conit.lives = self.lives
        return conit 


    def isRemovable(self):
        return self.hits < self.limit and self.lives