# -*- coding: utf-8 -*-
"""
See the README.md file for the prerequisites to run this file.

For the first run, set init_db to True, and then to False.
Running this file will send some tableaux d'avancement to the database, and
most likely send some CIDs to the failedTexts table.

Spyder encounters an issue with multiprocessing. If you want to run this file
from Spyder, it must be run in an external terminal. To do that: 
Run > Configuration per file > Execute in an external system terminal
"""
from converter import createTextProvider, createDbManager, Middleman

if __name__ == "__main__":
    from multiprocessing import Process, Pipe
    init_db = False
    runTest = True
    import secret
    from converter import SearchFilters, Markers
    if init_db:
        print("Initialising DB")
        from dbStructure import initDb
        from dbConnector import DbConnector
        initDb(DbConnector(secret.DB_NAME, secret.DB_USER, secret.DB_PW))
        print("DB initialised")
    if runTest:
        print("Setting up query process")
        legi1, legi2 = Pipe(True)
        db1, db2 = Pipe(True)
        command1, command2 = Pipe(True)
        legiProcess = Process(target=createTextProvider, 
                              args=((secret.CLIENT_ID, secret.CLIENT_SECRET),
                                    legi2))
        dbProcess = Process(target=createDbManager,
                            args=((secret.DB_NAME, secret.DB_USER, secret.DB_PW),
                                  db2))
        middleProcess = Process(target=Middleman.create,
                                args=(legi1, db1, command1))
        for query in SearchFilters:
            command2.send(query)
        command2.send(Markers.END)
        print("Starting processes")
        for p in (middleProcess, legiProcess, dbProcess):
            p.start()
        for p in (middleProcess, legiProcess, dbProcess):
            p.join()
        print("All done!")