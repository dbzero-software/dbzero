import dbzero as db0
from .dbzero import _compare


def compare(obj_1, obj_2, tags=None):
    if _compare(obj_1, obj_2):
        # if objects are identical then also compare tags
        if tags is not None:
            # retrieve associated snapshots since objects may come from different ones
            snap_1 = db0.get_snapshot_of(obj_1)
            snap_2 = db0.get_snapshot_of(obj_2)
            for tag in tags:
                if len(snap_1.find(tag, obj_1)) != len(snap_2.find(tag, obj_2)):
                    return False
        # tags are identical
        return True
                
    return False