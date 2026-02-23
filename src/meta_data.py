"""Metadata"""
import os

class PhotoMetaData():
    """PMD"""
    def __init__(self,  album, path, md5, photo=None, tup=None):
        self.photo = photo
        self.id = photo.id if photo else tup(0)
        self.filename = photo.filename if photo else tup(1)
        self.size = photo.size if photo else tup(2)
        self.filesize = 0
        self.created = photo.created.isoformat() if photo else tup(3)
        self.asset_date = photo.asset_date.isoformat() if photo else tup(4)
        self.added_date = photo.added_date.isoformat() if photo else tup(5)
        self.x = photo.dimensions[0] if photo else tup(6)
        self.y = photo.dimensions[1] if photo else tup(7)
        self.item_type = photo.item_type if photo else tup(8)
        self.item_type_extension = os.path.splitext(self.filename)
        self.path = path
        self.md5 = md5
        self.album = album

class AlbumMetaData():
    """AMD"""
    def __init__(self, name):
        self.name = name
        self.assets: list[PhotoMetaData] = []
