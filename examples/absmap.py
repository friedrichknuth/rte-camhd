import logging
from collections import MutableMapping
from google.cloud import storage
from zarr.util import normalize_storage_path

logger = logging.getLogger(__name__)

def strip_prefix_from_path(path, prefix):
    # assume path prefix *does* contain a trailing /
    if path.startswith(prefix):
        return path[len(prefix):]
    else:
        return path

class GCSMap(MutableMapping):

    def __init__(self, bucket_name, prefix, client_kwargs={}):

        self.bucket_name = bucket_name
        self.prefix = normalize_storage_path(prefix)
        self.client_kwargs = {}
        
        self.initialize_bucket()
        
    def initialize_bucket(self):
        client = storage.Client(**self.client_kwargs)
        self.bucket = client.get_bucket(self.bucket_name)
        
    # needed for pickling
    def __getstate__(self):
        state = self.__dict__.copy()
        del state['bucket']
        return state

    def __setstate__(self, state):
        self.__dict__.update(state)
        self.initialize_bucket()
        

    def __getitem__(self, key):
        logger.debug('__getitem__(%s)' % key)
        blob_name = '/'.join([self.prefix, key])
        blob = self.bucket.get_blob(blob_name)
        if blob:
            return blob.download_as_string()
        else:
            raise KeyError('Blob %s not found' % blob_name)

    def __setitem__(self, key, value):
        raise NotImplementedError

    def __delitem__(self, key):
        raise NotImplementedError

    def __contains__(self, key):
        logger.debug('__contains__(%s)' % key)
        blob_name = '/'.join([self.prefix, key])
        return self.bucket.get_blob(blob_name) is not None

    def __eq__(self, other):
        return (
            isinstance(other, GCSMap) and
            self.bucket_name == other.bucket_name and
            self.prefix == other.prefix
        )

    def keys(self):
        raise NotImplementedError

    def __iter__(self):
        raise NotImplementedError

    def __len__(self):
        raise NotImplementedError
        
    def list_gcs_directory_blobs(self, prefix):
        """Return list of all blobs under a gcs prefix."""
        return [blob.name for blob in
                self.bucket.list_blobs(prefix=prefix, delimiter='/')]

    # from https://github.com/GoogleCloudPlatform/google-cloud-python/issues/920#issuecomment-326125992
    def list_gcs_subdirectories(self, prefix):
        """Return set of all "subdirectories" from a gcs prefix."""
        iterator = self.bucket.list_blobs(prefix=prefix, delimiter='/')
        prefixes = set()
        for page in iterator.pages:
            prefixes.update(page.prefixes)
        # need to strip trailing slash to be consistent with os.listdir
        return [path[:-1] for path in prefixes]

    def list_gcs_directory(self, prefix, strip_prefix=True):
        """Return a list of all blobs and subdirectories from a gcs prefix."""
        items = set()
        items.update(self.list_gcs_directory_blobs(prefix))
        items.update(self.list_gcs_subdirectories(prefix))
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
        return sorted(self.list_gcs_directory(dir_path, strip_prefix=True))

    def rename(self, src_path, dst_path):
        raise NotImplementedErrror

    def rmdir(self, path=None):
        raise NotImplementedErrror

    def getsize(self, path=None):
        logger.debug('getsize %s' % path)
        dir_path = self.dir_path(path)
        size = 0
        for blob in self.bucket.list_blobs(prefix=dir_path):
            size += blob.size
        return size

    def clear(self):
        raise NotImplementedError