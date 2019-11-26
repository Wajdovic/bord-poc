import datetime


def serializer(obj):
    try:
        return obj.toJSON()
    except:
        if isinstance(obj, datetime.datetime):
            return obj.__str__()
        else:
            return obj.__dict__
