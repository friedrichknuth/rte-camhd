import logging
from collections import MutableMappin
from azure import storage
from zarr.util import normalize_storage_path

logger = logging.getLogger(__name__)

def strip_prefix_from_path(path, prefix):
    # assume path prefix *does* contain a trailing /
    if path.startswith(prefix):
        return path[len(prefix):]
    else:
        return path

class ABSMap(MutableMapping):

	def __init__(self, container_name, prefix, user, token):

        self.user = user
        self.token = token
		self.container_name = container_name
        self.prefix = normalize_storage_path(prefix)
        self.initialize_container()

    def initialize_container(self):
        self.client = storage.blob.BlockBlobService(self.user, self.token)
        # azure doesn't seem to be a way to initialize a container as google goes with get_bucket(). 
        # client needs to be used in functions and container name needs to be passed on.
        # could get rid of this function and consolidate. 

    # needed for pickling
    def __getstate__(self):
        state = self.__dict__.copy()
        del state['container']
        return state

    def __setstate__(self, state):
        self.__dict__.update(state)
        self.initialize_container()


    def __getitem__(self, key):
        logger.debug('__getitem__(%s)' % key) # not sure what logger returns. need to test live and adapt.
        blob_name = '/'.join([self.prefix, key])
        blob = self.client.get_blob_to_text(self.container_name, blob_name)
        if blob:
            return blob
        else:
            raise KeyError('Blob %s not found' % blob_name)

    def __setitem__(self, key, value):
        raise NotImplementedError

    def __delitem__(self, key):
        raise NotImplementedError

    def __contains__(self, key):
        logger.debug('__contains__(%s)' % key)
        blob_name = '/'.join([self.container_name, key])
        return self.client.get_blob_to_text(blob_name) is not None

    def __eq__(self, other):
        return (
            isinstance(other, ACSMap) and
            self.container_name == other.container_name and
            self.prefix == other.prefix
        )

    def keys(self):
        raise NotImplementedError

    def __iter__(self):
        raise NotImplementedError

    def __len__(self):
        raise NotImplementedError

    def __contains__(self, key):
        logger.debug('__contains__(%s)' % key)
        blob_name = '/'.join([self.prefix, key])
        return self.client.get_blob_to_text(blob_name) is not None

        
    def list_abs_directory_blobs(self, prefix):
        """Return list of all blobs under a abs prefix."""
        return [blob.name for blob in
                self.client.list_blobs(prefix=prefix)]

    def list_abs_subdirectories(self, prefix):
        """Return set of all "subdirectories" from a abs prefix."""
        iterator = self.client.list_blobs(prefix=prefix, delimiter='/')

        # here comes a hack. azure list_blobs() doesn't seems to have iterator.pages

        return set([blob.name.rsplit('/',1)[:-1][0] for blob in iterator  if '/' in blob.name])

    def list_abs_directory(self, prefix, strip_prefix=True):
        """Return a list of all blobs and subdirectories from a gcs prefix."""
        items = set()
        items.update(self.list_abs_directory_blobs(prefix))
        items.update(self.list_abs_subdirectories(prefix))
        items = list(items)
        if strip_prefix:
            items = [strip_prefix_from_path(path, prefix) for path in items]
        return items

    def dir_path(self, path=None):
        store_path = normalize_storage_path(path)
        # prefix is normalized to not have a trailing slash
        dir_path = self.prefix 
        if store_path:
            dir_path = '/'.join(dir_path, store_path)
        else: 
            dir_path += '/'
        return dir_path

    def listdir(self, path=None):
        logger.debug('listdir(%s)' % path)
        dir_path = self.dir_path(path)
        return sorted(self.list_abs_directory(dir_path, strip_prefix=True))

    def rename(self, src_path, dst_path):
        raise NotImplementedErrror

    def rmdir(self, path=None):
        raise NotImplementedErrror

    def getsize(self, path=None):
        logger.debug('getsize %s' % path)
        dir_path = self.dir_path(path)
        size = 0
        for blob in self.client.list_blobs(prefix=dir_path):
            size += blob.properties.content_length # from https://stackoverflow.com/questions/47694592/get-container-sizes-in-azure-blob-storage-using-python
        return size

    def clear(self):
        raise NotImplementedError
