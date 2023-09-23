import os
import uuid


def get_target_dir(path_name: str = ""):
    target_dir = os.path.join(os.getcwd(), "sink", str(uuid.uuid4()).upper(), path_name)
    try:
        if not os.path.isdir(target_dir):
            os.makedirs(target_dir, exist_ok=True)
    except Exception as e:
        print(e)

    return target_dir
