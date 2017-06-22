import json
import urllib2
import platform
import time, string
from bs4 import BeautifulSoup
from sys import argv

if len(argv) != 4:
    print 1
    exit(1)
else:
    domain = argv[1]
    cluster = argv[2]
    bits = int(argv[3])
    # bits = string.atof(argv[3])
#
# domain = '172.20.66.225'
# cluster = 'JMC-UAT'
# bits = 7

s = 0
message = []
# url = 'http://172.20.66.225:9000/clusters/JMC-UAT/consumers'
url = 'http://' + domain + ':9000/clusters/' + cluster + '/consumers'
request = urllib2.Request(url)
try:
    response = urllib2.urlopen(request, timeout=5)
except:
    print 1
    exit(1)
result = response.read()
data = {}
for row in BeautifulSoup(result, "html.parser")("tr"):
    s = ['lag', 'type', 'group']
    one = {}
    for cell in row("td"):
        text = cell.text.replace('\n', '')
        if text.find('console-consumer') + 1:
            break
        one[s.pop()] = text
    try:
        data[one['group'] + '(' + one['type'] + ')'] = one
    except:
        pass

# tmpfile: last time lags records.
# logfile: all lags entry history
# mailfile: extend message send to email.
# lagsumfile: get number to graphs

if platform.system().upper() == 'LINUX':
    tmpfile = '/tmp/.kafkalags'
    logfile = '/tmp/.kafkalagslog'
    mailfile = '/tmp/.kafkamail'
    lagsumfile = '/tmp/.kafkalagssum'
elif platform.system().upper() == 'WINDOWS':
    tmpfile = '.kafkalags'
    logfile = '.kafkalagslog'
    mailfile = '.kafkamail'
    lagsumfile = '.kafkalagssum'
else:
    exit(9)

try:
    ffff = open(tmpfile, 'r')
    xxxx = json.loads(ffff.readlines().pop())
    ffff.close()
except:
    pass


def writeLogs(chars):
    with open(logfile, 'a') as logfilehandle:
        logfilehandle.write(time.strftime('%Y-%m-%d %H:%M:%S ', time.localtime()) + '  ' + chars + "\n")


def son(chars, topicinput, lagsinput):
    if chars in ['VEHICLE-STATUS(ZK)', 'ev.convert.status.header.group(ZK)', 'dataParseConsumers1(ZK)', 'test(KF)',
                 'debug(ZK)', 'driveBehaviorCalConsumers1(KF)']:
        return False

    try:
        splits = xxxx[chars]['lag'][topicinput].split(',')  # .split(')')
    except:
        splits = [data[chars]['lag'][topicinput]]
        try:
            xxxx[chars]
        except:
            xxxx[chars] = {}

        try:
            xxxx[chars]['lag']
        except:
            xxxx[chars]['lag'] = {}

        # xxxx[chars] = {}
        # xxxx[chars]['lag'] = {}
        xxxx[chars]['lag'][topicinput] = lagsinput
        xxxx[chars]['group'] = data[chars]['group']
        xxxx[chars]['type'] = data[chars]['type']
        return False
    mark = 0
    for lag in splits:
        # if topic == topicinput:
        # if int(lagsinput) - int(lags) > int(lags) * bits:
        if int(lagsinput) >= int(lag) and int(lagsinput) > 0:
            mark += 1
        else:
            break
    lags = xxxx[chars]['lag'][topicinput] + ',' + lagsinput

    lags = lags.split(',')
    if len(lags) >= bits:
        xxxx[chars]['lag'][topicinput] = ','.join(lags[len(lags) - bits:])
    else:
        xxxx[chars]['lag'][topicinput] = ','.join(lags[0:])
    if mark < len(splits):
        # message.append(chars)
        return False
    elif mark == len(splits):
        writeLogs('group: ' + chars + ',topic: ' + topicinput + ",lag:" + lagsinput + ',history lags:' + str(lags))
        if not chars in message:
            message.append(chars)
        return True


for once in data:

    splits = data[once]['lag'].split(')')
    data[once]['lag'] = {}
    for topicsAndlag in splits:
        topic = str(topicsAndlag).split(':')[0].replace(' ', '')
        try:
            lags = str(topicsAndlag).split(':')[1]
        except:
            continue
        lags = lags.split('coverage,').pop().replace(' lag', '').replace(' ', '')

        data[once]['lag'][topic] = lags
        # if int(lags) > 10:
        try:
            # if son(xxxx[data[once]['group'] + '(' + data[once]['type'] + ')']['lag'], topic, lags):
            if son(data[once]['group'] + '(' + data[once]['type'] + ')', topic, lags):
                s = 1
        except Exception, e:
            writeLogs("Exception:" + str(e).replace('\n', ','))
            continue

with open(tmpfile, 'w') as ffff:
    try:
        ffff.write(json.dumps(xxxx))
    except Exception, e:
        ffff.write(json.dumps(data))

if s == 1:
    print 1
    with open(mailfile, 'w') as logfilehandle:
        logfilehandle.write(cluster + " check url: " + url + "\nkafka consumer group lags: " + ",".join(message) + "\n")
    with open(lagsumfile, 'w') as lagsumfilehandle:
        lagsumfilehandle.write(str(len(message)))
else:
    writeLogs('NO LAGS')
    with open(mailfile, 'w') as logfilehandle:
        logfilehandle.write("No Lags.\n")
    with open(lagsumfile, 'w') as lagsumfilehandle:
        lagsumfilehandle.write('0')
    print 0
writeLogs('=======================================================')
