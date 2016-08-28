from kazoo.client import KazooClient
import logging
import re
from subprocess import * 

logging.basicConfig(filename = "log", level = logging.INFO)

def get_column_infos(zk_host, root_path):
    column_infos = {}

    zk = KazooClient(hosts = zk_host)
    zk.start()

    if zk.exists(root_path):
        # Get all channels
        pattern = re.compile('^[a-z][0-9]+$')
        children = zk.get_children(root_path)
        for child in children:
            if pattern.match(child):
                logging.info("Find channel = %s", child)
                infos = get_channel_column_infos(zk, root_path, child)
                for k,v in infos.iteritems():
                    if k in column_infos:
                        column_infos[k] += v
                    else:
                        column_infos[k] = v
    else:
        logging.error("zk_host = %s, root_path = %s is not exist", zk_host, root_path)
        
    zk.stop()

    return column_infos

def parse_region_column_infos(info_str):
    column_infos = {}
    lines = info_str.split('\n')
    pattern = re.compile('^Column Infos: (.*)$')
    for line in lines:
        ret = pattern.match(line)
        if ret != None:
            items = ret.group(1)[1:-1].split(',')
            if len(items) == 0 and items[0] == '':
                logging.info("Column Infos is empty")
            else:
                for item in items:
                    item_split = item.split(':')
                    if len(item_split) != 2:
                        logging.error("Column Infos format error")
                        break
                    else:
                        column_infos[item_split[0]] = long(item_split[1])
            break 
    else:
        return None

    return column_infos

def get_channel_column_infos(zk, root_path, channel):
    column_infos = {}

    channel_path = root_path + "/" + channel
    regions = zk.get_children(channel_path)
    pattern = re.compile('^c[0-9]+_region[0-9]+$')
    for region in regions:
        if pattern.match(region):
            data,stat = zk.get(channel_path + '/' + region) 
            result = data.split(':')
            region_host = result[1]
            region_port = result[2]
            child = Popen(['redis-cli', '-h', region_host, '-p', region_port, "info", "column"], stdout = PIPE, stderr = PIPE)
            out, err = child.communicate()
            if child.returncode != 0:
                logging.error("Get region channel infos error. %s", err)
                continue
            else:
               # parse column infos
                info = parse_region_column_infos(out)
                if info == None:
                    logging.warn("Region host: %s do not support column infos", region_host)
                else:
                    for k,v in info.iteritems():
                        if k in column_infos:
                            column_infos[k] += v
                        else:
                            column_infos[k] = v
        else:
            logging.error("region = %s format error")
    
    return column_infos

if __name__ == '__main__':
    ret = get_column_infos('127.0.0.1:2181', '/pfc/mint/tc')
    # Test root path not exist
    #ret = get_column_infos('127.0.0.1:2181', '/pfc/mint/not_exist')
    print ret
