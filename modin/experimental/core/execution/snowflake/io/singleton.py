from snowflake.snowpark import Session

class SessionWraper(object):
    def __new__(cls, con_dict):
        if not hasattr(cls, 'instance'):
            cls.instance = super(SessionWraper, cls).__new__(cls)
            cls.instance.session = Session.builder.configs(con_dict).create()
        return cls.instance