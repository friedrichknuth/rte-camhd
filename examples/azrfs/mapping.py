from collections import MutableMapping

class AZRMap(object):

    def __init__(self, azr=None):
        self.azr = azr
        self.MutableMapping = MutableMapping
        
    def list_azr_directory_containers(self):
        return [container.name for container in self.azr.client.list_containers()]

    def list_azr_directory_blobs(self, container):
        return [blob.name for blob in self.azr.client.list_blobs(container)]