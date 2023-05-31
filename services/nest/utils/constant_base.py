

class ConstantBase:
    @classmethod
    def get_lookup(cls):
        attrb = dict()
        for key in cls.__dict__:
            if '_' not in key:
                attrb[cls.__dict__[key]] = key
        return attrb
