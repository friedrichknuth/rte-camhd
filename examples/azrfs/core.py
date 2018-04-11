from azure.storage.blob import BlockBlobService

class AZRFileSystem(object):
    
    def __init__(self, user, token):
        self.user = user
        self.token = token
        self.client = BlockBlobService(self.user, self.token)
        