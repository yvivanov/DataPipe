#!/usr/bin/python3 -u
# yvivanov@yahoo.com 2024
#   +nap time adjustment
#   +connection string encoded
#   +retro-scan includes "tomorrow"
#   +pyodbc is used to connect to MSSQL and MySQL https://github.com/mkleehammer/pyodbc/wiki
# pip install pyodbc PyYAML

import datetime, json, pyodbc, time, threading, yaml

def getYml(fileName):
    dict = None
    with open(fileName, "r") as file: dict = yaml.safe_load(file)
    return dict

def getListBySQL(connection, SQL):
    cursor = connection.cursor()
    cursor . execute(SQL)
    array  = []
    for row in cursor: array . append(row[0])
    cursor . close()
    return array

class Sync(threading.Thread):
    def __init__(self, threadName, counter, review, repeat, syncms, db_src, source, db_tgt, target, fields):
        threading.Thread.__init__(self)
        self.threadID   = counter
        self.threadName = threadName
        self.counter    = counter
        self.review     = review
        self.repeat     = repeat
        self.syncms     = syncms
        self.db_src     = db_src
        self.source     = source
        self.db_tgt     = db_tgt
        self.target     = target
        self.fields     = fields
    def run(self):
        exe(self.threadName, self.counter, self.review, self.repeat, self.syncms, self.db_src, self.source, self.db_tgt, self.target, self.fields)

def retrospectiveUpdate(inflow, output, source, target, db_src, db_tgt, idName, upName, selectSql, replaceSql):
    try:  # include tomorrow for IST datetime: PST + 11.30
        datediffSql = "select " + idName + " from " + source + " where datediff(day," + upName + ",current_timestamp)<="              + actuality + " order by " + idName if db_src == 'mssql' \
                 else "select " + idName + " from " + source + " where datediff(date_add(curdate(),interval 1 day)," + upName + ")<=" + actuality + " order by " + idName
        sourceIdList = getListBySQL(inflow, datediffSql)
        for item in sourceIdList:
            item = str(item)
            if db_src == 'mssql': sourceCursor = inflow.cursor(); sourceCursor.execute("select datediff(s,'1970-01-01 00:00:00'," + upName + ')from ' + source + ' where ' + idName + '=' + item)
            else:                 sourceCursor = inflow.cursor(); sourceCursor.execute('select unix_timestamp(' + upName +                   ')from ' + source + ' where ' + idName + '=' + item)
            # target is always mysql
            targetCursor = output.cursor(); targetCursor.execute('select unix_timestamp(' + upName + ') from ' + target + ' where ' + idName + '=' + item)
            sourceData = sourceCursor.fetchone()
            targetData = targetCursor.fetchone()
            if sourceData is not None and targetData is not None:
                sourceUpdated = int(sourceData[0])
                targetUpdated = int(targetData[0])
                sourceCursor.close()
                targetCursor.close()
                if abs(sourceUpdated + GMT_local - targetUpdated) >= precision:
                    sourceCursor = inflow.cursor(); sourceCursor.execute(selectSql.replace('in(?)', ('=' + item)))
                    targetCursor = output.cursor(); targetCursor.execute(replaceSql, sourceCursor.fetchone())
                    sourceCursor.close()
                    targetCursor.close()
                    print('~ REPLACED\t' + target + '\t' + item + '\t' + str(sourceUpdated) + '\t' + str(targetUpdated))
                    output.commit()
            else: print('= WARNING\t' + target + '\t' + item + '\tnot defined\t' + upName)
        print('=\t\t ' + source + '\t' + str(len(sourceIdList)) + '\trescanned')
    except (pyodbc.Error, pyodbc.DatabaseError) as error:
        output.rollback()
        print('* ROLLBACK on replace', target, error)
    sourceIdList = None

def exe(threadName, counter, review, repeat, syncms, db_src, source, db_tgt, target, fields):
    print('+', counter, threadName)
    inflow = pyodbc.connect(connectMS) if db_src == 'mssql' else pyodbc.connect(connectMy)
    output = pyodbc.connect(connectMS) if db_tgt == 'mssql' else pyodbc.connect(connectMy)
    output . autocommit = False
    idName = fields[0]
    upName = fields[len(fields) - 1]

    i = 0
    fnames = ''
    string = '('
    for field in fields:
        fnames += ',' + field if i > 0 else field
        string += ',?'        if i > 0 else '?'
        i += 1
    string += ')'

    selectSql  = 'select '      + fnames + ' from ' + source + ' where ' + idName + ' in(?)'
    insertSql  = 'insert into ' + target + '(' + fnames + ')values' + string
    replaceSql = 'replace '     + target + '(' + fnames + ')values' + string

    n = 0
    while n < repeat:
        n += 1
        start = int(time.time())
        sourceCursor = inflow.cursor(); sourceCursor.execute('select count(*) from ' + source); sourceSize = sourceCursor.fetchone()[0]; sourceCursor.close()
        targetCursor = output.cursor(); targetCursor.execute('select count(*) from ' + target); targetSize = targetCursor.fetchone()[0]; targetCursor.close()
        print(str(n) + '\t' + ('\t ' if sourceSize == targetSize else '\t*') + str(sourceSize) + '\t' + str(targetSize) + '\t' + source + '\t' + target)

        if sourceSize == targetSize:
            # update target for the actuality interval from source
            if review: retrospectiveUpdate (inflow, output, source, target, db_src, db_tgt, idName, upName, selectSql, replaceSql)
        elif sourceSize > targetSize:
            # calculate source *minus* target rows, then insert them to the target table
            sourceIdList = getListBySQL(inflow, 'select ' + idName + ' from ' + source + ' order by ' + idName)
            targetIdList = getListBySQL(output, 'select ' + idName + ' from ' + target + ' order by ' + idName)
            sourceIdList = list(set(sourceIdList) - set(targetIdList))

            rowsInsert = len(sourceIdList)
            chunkCount = rowsInsert // chunkSize
            chunkExtra = rowsInsert %  chunkSize
            try:
                output.autocommit = False
                index = 0
                for i in range(0, chunkCount):
                    chunk = sourceIdList[index : index + chunkSize]; index += chunkSize
                    inSql = '('
                    for j in range(0, chunkSize): inSql += ',' + str(chunk[j]) if j > 0 else str(chunk[j])
                    inSql += ')'
                    fixSelectSql = selectSql.replace('(?)', inSql)
                    sourceCursor = inflow.cursor()
                    sourceCursor . execute(fixSelectSql)
                    targetCursor = output.cursor()
                    targetCursor . executemany(insertSql, sourceCursor.fetchall())
                    sourceCursor . close()
                    targetCursor . close()
                    output.commit()
                if chunkExtra > 0:
                    chunk = sourceIdList[index : index + chunkExtra]
                    inSql = '('
                    for j in range(0, chunkExtra): inSql += ',' + str(chunk[j]) if j > 0 else str(chunk[j])
                    inSql += ')'
                    fixSelectSql = selectSql.replace('(?)', inSql)
                    sourceCursor = inflow.cursor()
                    sourceCursor.execute(fixSelectSql)
                    targetCursor = output.cursor()
                    targetCursor.executemany(insertSql, sourceCursor.fetchall())
                    sourceCursor.close()
                    targetCursor.close()
                    output.commit()
            except (pyodbc.Error, pyodbc.DatabaseError) as error:
                output.rollback()
                print('* ROLLBACK on insert', target, error)
                break
            if review: retrospectiveUpdate (inflow, output, source, target, db_src, db_tgt, idName, upName, selectSql, replaceSql)
            sourceIdList = None
            targetIdList = None
            chunk        = None
        else:
            print('* ABNORMAL source table is fewer than target')
            break
        runTime = int(time.time()) - start
        napTime = syncms // 1000 - runTime
        if napTime <= 0: print('.\t\t', str(runTime) + 's\t' + target)
        else:            time.sleep(napTime)

    if db_src == db_tgt:  # same database
        inflow.close()
    else:
        inflow.close()
        output.close()
    print('-', counter, threadName)
# ----------------------------------------------------------------------------------------------------------------------
config    = getYml('DataPipe-TBOSRPT.yml')
connectMS = config["connectMS"]
connectMy = config["connectMy"]

inflow = pyodbc.connect(connectMS)
cursMS = inflow.cursor()
cursMS . execute('select @@VERSION')
print('MSSQL', cursMS.fetchone()[0])
cursMS . close()
inflow . close()
output = pyodbc.connect(connectMy)
cursMy = output.cursor()
cursMy . execute('select @@VERSION')
print('MySQL', cursMy.fetchone()[0])
cursMy . close()
output . close()

actuality = str(config['actuality'])
precision = config['precision']
GMT_local = config['GMT_local']
chunkSize = config['chunksize']

pool = []  # multi-threading
for i in range(100, 200):
    suffix = '_' + str(i)[-2:]
    submit = 'submit' + suffix
    if submit in config and config[submit]:
        thread = config['source' + suffix]
        review = config['review' + suffix]
        repeat = config['repeat' + suffix]
        syncms = config['syncms' + suffix]
        db_src = config['db_src' + suffix]
        source = config['source' + suffix]
        db_tgt = config['db_tgt' + suffix]
        target = config['target' + suffix]
        fields = config['fields' + suffix]

        sync = Sync(thread, i - 100, review, repeat, syncms, db_src, source, db_tgt, target, fields)
        sync . start()
        pool . append(sync)

time.sleep(1)
for sync in pool:
    sync . join()

print('OK')

"""
+ 1 AUDIT.ADT_INTERNAL_USER_LOGINS
+ 2 AUDIT.ADT_PAGETRACKER
+ 3 AUDIT.ADT_USEREVENTS_TRACKER
1		*85513	0	AUDIT.ADT_INTERNAL_USER_LOGINS	AUDIT.ADT_INTERNAL_USER_LOGINS
1		*591415	0	AUDIT.ADT_PAGETRACKER	AUDIT.ADT_PAGETRACKER
1		*879274	0	AUDIT.ADT_USEREVENTS_TRACKER	AUDIT.ADT_USEREVENTS_TRACKER
.		 409s	AUDIT.ADT_INTERNAL_USER_LOGINS
.		 615s	AUDIT.ADT_PAGETRACKER
.		 1506s	AUDIT.ADT_USEREVENTS_TRACKER

FortiClient VPN:
    Pinging sr91qacsc.tollplus.com [192.168.120.61] with 32 bytes of data:
        Approximate round trip times in milli-seconds:
        Minimum = 559ms, Maximum = 584ms, Average = 567ms
*unacceptable*
"""
