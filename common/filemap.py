"""Filemap class - path mapping for client storage. No dbutils dependency."""

MOUNT_POINT = "/mnt/msd-datalake"


class Filemap:
    """
    Provides path details for each client storage layout.
    client: string, e.g. "TSE", "AFHU"
    """
    def __init__(self, client):
        self.MOUNT = '/dbfs' + MOUNT_POINT + '/'
        self.RAW = self.MOUNT + 'Raw/' + client + '/'
        self.STAGED = self.MOUNT + 'Staged/' + client + '/'
        self.CURATED = self.MOUNT + 'Curated/' + client + '/'
        self.ARCHIVE = self.MOUNT + 'Archive/' + client + '/'
        self.HOME = '/databricks/driver/'
        self.SHARED = self.MOUNT + 'Shared/'
        self.SCHEMA = self.MOUNT + 'Schema/' + client + '/'
        self.SUPPRESSIONS = self.MOUNT + 'Suppressions/' + client + '/'
        self.MASTER = self.MOUNT + 'Master/' + client + '/'
