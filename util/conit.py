class Conit:
    """
    Consistency unit, keep a resource and some metrics.
    """
    def __init__(self, data, limit=5, owner=None):
        self.data = data
        self.hits = 0
        self.lives = 0
        self.limit = limit
        self.owners = list() if owner is None else [owner]

    
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
        self.owners.append(owner)

    
    def removeOwner(self, owner):
        self.owners.remove(owner)

    
    def addLive(self)
        self.lives += 1