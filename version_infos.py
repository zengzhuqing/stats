from kazoo.client import KazooClient
import logging
import re
from subprocess import *
import json

logging.basicConfig(filename = "log", level = logging.INFO)

class KutypeValue:
    def __init__(self):
        self.size = 0
        self.version_dict = {}

    def add_version(self, version, timestamp, size):
        self.size += size
        if version in self.version_dict:
            v = self.version_dict[version]
            v[1] += size
        else:
            v = []
            v.append(timestamp)
            v.append(size)
            self.version_dict[version] = v

    def add_kutype_value(self, kutype_value):
        for k,v in kutype_value.version_dict.iteritems():
            self.add_version(k, v[0], v[1])

def get_version_infos(zk_host, root_path):
    version_infos = {}

    zk = KazooClient(hosts = zk_host)
    zk.start()

    if zk.exists(root_path):
        # Get all channels
        pattern = re.compile('^[a-z][0-9]+$')
        children = zk.get_children(root_path)
        for child in children:
            if pattern.match(child):
                logging.info("Find channel = %s", child)
                infos = get_channel_version_infos(zk, root_path, child)
                for k,v in infos.iteritems():
                    if k in version_infos:
                        version_infos[k].add_kutype_value(v)
                    else:
                        version_infos[k] = v
    else:
        logging.error("zk_host = %s, root_path = %s is not exist", zk_host, root_path)
       
    zk.stop()

    result = {}
    for k, v in version_infos.iteritems():
        vv  = {}
        vv['size'] = v.size
        vv['version_dict'] = v.version_dict
        result[k] = vv

    return result
    #return version_infos

def parse_region_version_infos(info_str):
    version_infos = {}
    lines = info_str.split('\n')
    pattern = re.compile('^kutype.*$')
    begin = False
    for line in lines:
        if not begin:
            ret = pattern.match(line)
            if ret != None:
                begin = True
        else:
            if len(line) == 0:
                break
            else:
                ret = line.split(':')
                if len(ret) != 2:
                    logging.error("version format error")
                else:
                    idx = ret[0].find('(')
                    kutype = long(ret[0][0:idx])
                    ku_value = KutypeValue()
                    if len(ret[1]) <= 4 :
                        logging.error("version format error")
                        continue
                    for item in ret[1][1:-2].split('),'):
                        jdx = item.find('(')
                        kdx = item.find(',')
                        version = long(item[0:jdx])
                        timestamp = long(item[jdx + 1:kdx])
                        size = long(item[kdx+1:])
                        ku_value.add_version(version, timestamp, size)
                    version_infos[kutype] = ku_value

    if begin == False:
        return None

    return version_infos

def get_channel_version_infos(zk, root_path, channel):
    version_infos = {}

    channel_path = root_path + "/" + channel
    regions = zk.get_children(channel_path)
    pattern = re.compile('^c[0-9]+_region[0-9]+$')
    for region in regions:
        if pattern.match(region):
            data,stat = zk.get(channel_path + '/' + region)
            result = data.split(':')
            region_host = result[1]
            region_port = result[2]
            child = Popen(['redis-cli', '-h', region_host, '-p', region_port, "info", "version"], stdout = PIPE, stderr = PIPE)
            out, err = child.communicate()
            if child.returncode != 0:
                logging.error("Get region channel infos error. %s", err)
                continue
            else:
               # parse version infos
                info = parse_region_version_infos(out)
                if info == None:
                    logging.warn("Region host: %s do not support version infos", region_host)
                else:
                    for k,v in info.iteritems():
                        if k in version_infos:
                            version_infos[k].add_kutype_value(v)
                        else:
                            version_infos[k] = v
        else:
            logging.error("region = %s format error")
 
    return version_infos

if __name__ == '__main__':
    ret = get_version_infos('127.0.0.1:2181', '/pfc/mint/tc')
    # Test root path not exist
    #ret = get_column_infos('127.0.0.1:2181', '/pfc/mint/not_exist')
    print ret
    print json.dumps(ret)
