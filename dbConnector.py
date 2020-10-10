# -*- coding: utf-8 -*-
"""
Provide a way to request the PostgreSQL database to store the parsing results.

Classes
-------
DbConnector
    Class handling the connection to the DB.
"""
import psycopg2, psycopg2.extras

class DbConnector:
    """
    Class handling the connection to the DB. 
    
    SHOULD be used as a context 
    manager in a with-block for several transactions in a row but will work 
    fine even otherwise.
    """
    def __init__(self, dbname, user, password, host="127.0.0.1", port="5432"):
        """
        Create an object ready to handle the connection to the DB.

        Parameters
        ----------
        dbname : str
            Name of the database to connect to.
        user : str
            Login of the user in the database.
        password : str
            Password of the user in the database.
        host : str, optional
            IP address of the database. The default is "127.0.0.1".
        port : str or int, optional
            Port of the database at the IP address. The default is "5432".

        Returns
        -------
        A DbConnector object.
        """
        self._dbname = dbname
        self._user = user
        self._password = password
        self._host = host
        self._port = port
        self._connection = None
        self._nesting = 0
    
    def __enter__(self):
        if(self._nesting):
            self._nesting += 1
            return self
        else:
            self._nesting = 1
            self._connection = psycopg2.connect(dbname=self._dbname,
                                                user=self._user,
                                                password=self._password,
                                                host=self._host,
                                                port=self._port)
            self._connection.__enter__()
            return self
    
    def __exit__(self, exc_type, exc_value, exc_traceback):
        self._nesting -= 1
        if not self._nesting:
            self._connection.__exit__(exc_type, exc_value, exc_traceback)
            self._connection = None
        
    def execute(self, query):
        """
        Silently execute the given query against the database.

        Parameters
        ----------
        query : str
            PostgreSQL query.

        Returns
        -------
        None.
        """
        with self:
            with self._connection.cursor() as cursor:
                cursor.execute(query)
    
    def executeMany(self, query, params):
        """
        Silently execute the given parameterized query against the database.
        
        This method is particularly useful when a large number of queries must 
        be executed.

        Parameters
        ----------
        query : str
            Parameterized PostgreSQL query, where each parameter is of the form
            $i with i a counter starting at 1.
        params : list or tuple
            List of sets of parameters to use in the query. Each element must 
            have as many elements as there are parameter placeholders in the
            query.

        Returns
        -------
        None.
        """
        if len(params) == 0:
            return None
        prepared = query.strip().startswith("EXECUTE")
        placeholder = " (" + ",".join(["%s" for x in range(len(params[0]))]) + ")"
        if not prepared:
            mainQuery = "EXECUTE stmt" + placeholder
        else:
            mainQuery = query
        with self:
            with self._connection.cursor() as cursor:
                if not prepared:
                    cursor.execute("PREPARE stmt AS " + query)
                psycopg2.extras.execute_batch(cursor, mainQuery, params)
                if not prepared:
                    cursor.execute("DEALLOCATE stmt")
                self._executeMany(query, params)
        
                
    def executeAndFetch(self, query, args=None):
        """
        Execute the given query against the database and return the results.

        Parameters
        ----------
        query : str
            PostgreSQL query.
        args : tuple, optional
            Arguments to pass to the SQL query. The default is None.

        Returns
        -------
        list
            The list of results of the query: each element is a tuple 
            containing the elements of a row returned by the query.
        """
        with self:
            with self._connection.cursor() as cursor:
                cursor.execute(query, args)
                return cursor.fetchall()
    
    def commit(self):
        """Commit pending transactions to the database."""
        self._connection.commit()