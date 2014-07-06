from os.path import dirname, abspath, join


def conf():
    _dir = dirname(dirname(abspath('config.py')))
    _path = join(_dir, 'postgis.conf')
    with open(_path, 'r') as _conf:
        postgis_conf = _conf.readlines()
    _conf.close()

    connection_setting = []
    for item in postgis_conf:
        connection_setting.append(item.split('=')[1])

    db_host = connection_setting[0][:-1]
    db_port = connection_setting[1][:-1]
    db_name = connection_setting[2][:-1]
    db_user = connection_setting[3][:-1]
    db_password = connection_setting[4]

    connection_string = "host=%s port=%s dbname=%s user=%s password=%s" % (
        db_host, db_port, db_name, db_user, db_password)
    return connection_string


