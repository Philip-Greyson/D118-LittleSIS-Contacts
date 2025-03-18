"""Script to send parent contact email info to LittleSIS.

https://github.com/Philip-Greyson/D118-LittleSIS-Contacts

Needs oracledb: pip install oracledb --upgrade
Needs pysftp: pip install pysftp --upgrade
"""

# importing modules
import datetime  # used to get current date for course info
import os  # needed to get environement variables
from datetime import datetime

import oracledb  # used to connect to PowerSchool database
import pysftp  # used to connect to the Versatrans SFTP server and upload the file

DB_UN = os.environ.get('POWERSCHOOL_READ_USER')  # username for read-only database user
DB_PW = os.environ.get('POWERSCHOOL_DB_PASSWORD')  # the password for the database account
DB_CS = os.environ.get('POWERSCHOOL_PROD_DB')  # the IP address, port, and database name to connect to in format x.x.x.x:port/db

#set up sftp login info, stored as environment variables on system
SFTP_UN = os.environ.get('LITTLESIS_SFTP_USERNAME')  # username for the SFTP upload
SFTP_PW = os.environ.get('LITTLESIS_SFTP_PASSWORD')
SFTP_HOST = os.environ.get('LITTLESIS_SFTP_ADDRESS')  # server address for the SFTP upload
CNOPTS = pysftp.CnOpts(knownhosts='known_hosts')  # connection options to use the known_hosts file for key validation

print(f'DB Username: {DB_UN} | DB Password: {DB_PW} | DB Server: {DB_CS}')  # debug so we can see where oracle is trying to connect to/with
print(f'SFTP Username: {SFTP_UN} | SFTP Server: {SFTP_HOST}')  # debug so we can see what info sftp connection is using

OUTPUT_FILENAME = 'littlesis_guardian.csv'
STUDENT_EMAIL_SUFFIX = '@d118.org'

if __name__ == '__main__':  # main file execution
    with open('littlesis_guardians_log.txt', 'w') as log:  # open logging file
        startTime = datetime.now()
        print(f'INFO: Execution started at {startTime}')
        print(f'INFO: Execution started at {startTime}', file=log)
        with open(OUTPUT_FILENAME, 'w', encoding='utf-8') as output:
            try:
                with oracledb.connect(user=DB_UN, password=DB_PW, dsn=DB_CS) as con:  # create the connecton to the database
                    print(f'INFO: Connection successfully established to PowerSchool at {DB_CS} on version {con.version}')
                    print(f'INFO: Connection successfully established to PowerSchool at {DB_CS} on version {con.version}', file=log)
                    with con.cursor() as cur:  # start an entry cursor
                        cur.execute('SELECT dcid, student_number, grade_level, schoolid FROM students WHERE enroll_status = 0')
                        students = cur.fetchall()
                        for student in students:
                            try:
                                stuDCID = int(student[0])
                                stuNum = int(student[1])
                                grade = int(student[2])
                                schoolNum = int(student[3])
                                print(f'{stuNum} - {stuDCID}', file=log)
                                cur.execute('SELECT ca.contactpriorityorder, p.firstname, p.lastname, codeset.code, cd.iscustodial, cd.liveswithflg, cd.schoolpickupflg, cd.isemergency, cd.receivesmailflg, ca.personid, gua.accountidentifier, gua.guardianid FROM studentcontactassoc ca INNER JOIN studentcontactdetail cd ON cd.studentcontactassocid = ca.studentcontactassocid AND cd.isactive = 1 LEFT JOIN codeset ON ca.CURRRELTYPECODESETID = codeset.codesetid LEFT JOIN person p ON ca.personid = p.id LEFT JOIN guardianpersonassoc gpa on ca.personid = gpa.personid LEFT JOIN guardian gua ON gpa.guardianid = gua.guardianid WHERE ca.studentdcid = :dcid ORDER BY cd.isemergency DESC, cd.iscustodial DESC, ca.contactpriorityorder', dcid=stuDCID)
                                contacts = cur.fetchall()
                                print(len(contacts), file=log)
                                print(contacts, file=log)
                            except Exception as er:
                                print(f'ERROR while doing general processing on student {student[1]}: {er}')
                                print(f'ERROR while doing general processing on student {student[1]}: {er}', file=log)
            except Exception as er:
                print(f'ERROR while connecting to PS or doing intial query: {er}')
                print(f'ERROR while connecting to PS or doing intial query: {er}', file=log)
