# -*- coding: utf-8 -*-
"""
Provide a converter between the Legifrance an DB formats.

Classes
-------
Markers
    Markers to send through pipes to communicate between processes.
Middleman
    Middleman between the main execution, the Legifrance and the DB processes.

Methods
-------
createTextProvider
    Create a listener ready to transfer texts from Legifrance to a pipe.
createDbManager
    Create a listener ready to query the database.
"""
from enum import Enum
from multiprocessing import connection
from legiConnector import LegiConnector
from dbConnector import DbConnector
from dbStructure import Types, Statements, prepareStatements
from legiStructure import SearchFilters

class Markers(Enum):
    """
    Markers to send through pipes to communicate between processes. 
    
    The only marker that SHOULD be used (except by the textProvider and 
    DbManager agents) is END.
    """
    END = "__END__"
    TEXT_LIST = "__TEXT_LIST__"
    TEXT = "__TEXT__"

def _parseText(text, criteria = SearchFilters.TAFilter):
    """
    Parse the content of a text to extract the data of interest.

    Parameters
    ----------
    text : dict
        MUST contain a key "content" containing the text as returned by
        the Legifrance API. This is consistent with the return value of 
        the getText method.
    criteria : SearchFilters, optional
        Filter through which the text was retrieved. This is used to 
        determine which pattern to match the text against.
        The default is SearchFilters.TAFilter.

    Returns
    -------
    dict
        Contains at least two keys: "success", which is True iff the text was 
        successfully parsed and False otherwise, and Types.cid, 
        associated with the CID of the text. If "success" is True, 
        it also contains Types.publicationDate (associated with a Unix 
        timestamp of the publication date of the text) and "data"
        (associated with a list of the parsed data from the text in the format
        expected by the insertRecord query).
        The value associated with "data" MAY be an empty string if every record
        has been ignored.
    """
    patterns = criteria.structs
    tmpResult = None
    index = -1
    while tmpResult is None and index < len(patterns)-1:
        index += 1
        tmpResult = patterns[index].match(text["content"])
    if tmpResult is None:
        return {Types.cid:text[Types.cid], "success":False}
    else:
        return {Types.cid:text[Types.cid], 
                Types.publicationDate: text[Types.publicationDate]//1000,
                "data":list(patterns[index].prepareForInsertion(tmpResult, 
                                                           text[Types.cid])), 
                "success":True}

def createTextProvider(args, pipeEnd):
    """
    Create a listener ready to transfer texts from Legifrance to a pipe.
    
    This method is blocking and SHOULD be used in a separate process. The
    object will listen to its pipeEnd to receive orders to get texts from 
    Legifrance. 
    The orders SHOULD be either Markers.END to signal that the
    process can end or a tuple (value from Markers, args corresponding to
    the marker).
    
    Markers.TEXT_LIST: associated with a SearchFilters object, get a list
    of CIDs from Legifrance, followed by a last message (Markers.END, filter).
    
    Markers.TEXT: associated with a CID or a list of CIDs, get a (list of) 
    text(s) from Legifrance.

    Parameters
    ----------
    args : tuple
        Arguments to create a LegiConnector object.
    pipeEnd : multiprocessing.connection.Connection
        This connection MUST be read/write. The easiest way to get
        such an object is to use one of the return values of 
        multiprocessing.Pipe(True).
    """
    connector = LegiConnector(*args)
    order = pipeEnd.recv()
    while order != Markers.END:
        if(order[0] == Markers.TEXT_LIST):
            for textList in _getTextIdList(connector, order[1]):
                pipeEnd.send((Markers.TEXT_LIST, order[1], textList))
            pipeEnd.send((Markers.END, order[1]))
        elif order[0] == Markers.TEXT:
            for text in _getText(connector, order[1]):
                pipeEnd.send((Markers.TEXT, _filterLegiText(text)))
        order = pipeEnd.recv()
    
def _getTextIdList(legiConnector, criteria = SearchFilters.TAFilter):
    """
    Get a list of text IDs from Legifrance and yield them by blocks.
    
    Use the Legifrance API to get a list of text CIDs corresponding to the 
    input search criteria. Assume that the input is well-formed and do not
    handle errors otherwise. The results are yielded by blocks as a list where 
    each element is the CID of a text matching the input criteria.

    Parameters
    ----------
    legiConnector : LegiConnector
        Connection to Legifrance.
    pipeEnd : multiprocessing.connection.Connection
        This connection MUST be writable, it MAY be write-only (i.e. 
        its only method of interest is send(obj)). The easiest way to get
        such an object is to use the second return value of 
        multiprocessing.Pipe(bool), where the boolean MAY be False.
    criteria : SearchFilters, optional
        Filter determining which type of texts to retrieve. 
        The default is SearchFilters.TAFilter.
    
    Yields
    ------
    List
        List of CIDs of texts matching the search filter.
    """
    pageNumber, currentResultNumber = 1, 0
    pageNumber, currentResultNumber, totalResultNumber, textList = \
        _getTextIdListHelper(pageNumber, criteria, legiConnector,
                             currentResultNumber)
    yield textList
    while currentResultNumber < totalResultNumber:  
        pageNumber, currentResultNumber, totalResultNumber, textList = \
            _getTextIdListHelper(pageNumber, criteria, legiConnector, 
                                 currentResultNumber)
        yield textList

def _getTextIdListHelper(pageNumber, criteria, legiConnector, resultNumber):
    """
    Get a page of results from Legifrance.

    Parameters
    ----------
    pageNumber : int
        Page to get from Legifrance
    criteria : SearchFilters
        Filter determining which type of texts to retrieve.
    legiConnector : LegiConnector
        Connection to Legifrance.
    resultNumber : int
        Current number of results returned so far or this filter.

    Returns
    -------
    pageNumber : int
        Next page to get from Legifrance.
    resultNumber : int
        Number of results returned so far, incremented by the number of results
        fetched by this method.
    totalResultNumber : int
        Total number of results to fetch from Legifrance.
    textList : list
        List of CIDs of texts from the page of results.
    """
    currentCriteria = criteria.payload
    currentCriteria["recherche"]["pageNumber"] = pageNumber
    results = legiConnector.post("/search", currentCriteria)
    totalResultNumber = results["totalResultNumber"]
    resultNumber += len(results["results"])
    currentCriteria["recherche"].pop("pageNumber")
    pageNumber += 1
    return pageNumber, resultNumber, totalResultNumber, \
        [x["titles"][0]["cid"] for x in results["results"]]

def _getText(legiConnector, cid):
    """
    Retrieve one or more text based on its/their CID(s).

    Parameters
    ----------
    legiConnector : LegiConnector
        Connection to Legifrance.
    cid : str or list
            CID of the text to retrieve, or list of CIDs of texts to retrieve.
    
    Yields
    ------
    dict
        The text corresponding to the Legifrance response.
    """
    if isinstance(cid, list):
        retrieve = cid
    else:
        retrieve = [cid]
    for e in retrieve:
        result = legiConnector.post("/consult/jorf", {"textCid":e})
        yield result
        
def _filterLegiText(text):
    """
    Filter the Legifrance response to a /consult/jorf query.

    Parameters
    ----------
    text : dict
        The Legifrance response to a consult/jorf/ query.

    Returns
    -------
    dict
        The CID (Types.cid), publication date (Types.publicationDate), 
        as a timestamp in milliseconds, and content ("content") of the text
        in a HTML encoding.
    """
    return {Types.cid: text["cid"],
            Types.publicationDate: text["dateParution"],
            #"articles" is a list that seems to always have only 1 element?!
            "content": (text["articles"][0]["content"] 
                        if len(text["articles"]) == 1 else None)}

def createDbManager(args, pipeEnd):
    """
    Create a listener ready to query the database.
    
    This method is blocking and SHOULD be used in a separate process. The
    object will listen to its pipeEnd to receive orders to query the database.
    The orders SHOULD be either Markers.END to signal that the
    process can end or a tuple (value from Markers, args corresponding to
    the marker).
    
    Markers.TEXT_LIST: associated with a list of CIDs, check if they are 
    already stored in the database.
    
    Markers.TEXT: associated with a list of parsed texts, store them in the
    database.

    Parameters
    ----------
    args : tuple
        Arguments to create a LegiConnector object.
    pipeEnd : multiprocessing.connection.Connection
        This connection MUST be read/write. The easiest way to get
        such an object is to use the second return value of 
        multiprocessing.Pipe(True).
    """
    order = pipeEnd.recv()
    with DbConnector(*args) as connector:
        prepareStatements(connector)
        while order != Markers.END:
            if order[0] == Markers.TEXT_LIST:
                _checkIfKnown(connector, pipeEnd, order[1])
            elif order[0] == Markers.TEXT:
                _storeText(connector, pipeEnd, order[1])
            order = pipeEnd.recv()

def _checkIfKnown(dbConnector, pipeEnd, cidList):
    """
    Check if one or more texts are already stored in the database.
    
    Query the database to check if it already contains the input CID(s).
    The results are pushed through the pipe whose end is given as input: 
    a single message, a tuple containing three elements: Markers.TEXT_LIST,
    the first cid in the query, and a list containing as elements the CIDs 
    that were not found in the database.

    Parameters
    ----------
    dbConnector : DbConnector
        Connection to the database.
    pipeEnd : multiprocessing.connection.Connection
        This connection MUST be writable, it MAY be write-only (i.e. 
        its only method of interest is send(obj)). The easiest way to get
        such an object is to use the second return value of 
        multiprocessing.Pipe(bool), where the boolean MAY be False.
    cidList : str or list of str
            CID of the text to check, or list of CIDs of texts to check.

    Returns
    -------
    None.
    """
    if isinstance(cidList, str):
        cidList = [cidList]
    refCid = cidList[0]
    for i in range(len(cidList)):
        cid = cidList[i]
        result = dbConnector.executeAndFetch(Statements.selectCid.query, (cid,))
        if result[0][0]:
            cidList[i] = None
    pipeEnd.send((Markers.TEXT_LIST, refCid, 
                  [x for x in cidList if x is not None]))

def _storeText(dbConnector, pipeEnd, texts):
    """
    Store the input parsed text(s) in the database.
    
    For each text stored in the database, a message (Markers.TEXT, cid) will
    be sent through pipeEnd. When all the texts have been stored, a message
    (Markers.END) will be sent as well.

    Parameters
    ----------
    dbConnector : DbConnector
        Connection to the database.
    pipeEnd : multiprocessing.connection.Connection
        This connection MUST be writable, it MAY be write-only (i.e. 
        its only method of interest is send(obj)). The easiest way to get
        such an object is to use the second return value of 
        multiprocessing.Pipe(bool), where the boolean MAY be False.
    texts : tuple, list or dict
        Each element MUST be a dict with keys "success" (bool) and Types.cid.
        If "success" is True, the element must also have keys 
        Types.publicationDate (int) and "data", itself a list with the values
        ordered as required by Statements.insertRecord.
        If the input is a dict, it will be treated as a one-element list.

    Returns
    -------
    None.
    """
    if isinstance(texts, dict):
        texts = [texts]
    success = []
    failure = []
    for text in texts:
            if text["success"]:
                success.append(text)
            else:
                failure.append(text[Types.cid])
    if failure:
        dbConnector.executeMany(Statements.insertFailed.query, 
                                [(x,) for x in failure])
    if success:
        dbConnector.executeMany(Statements.insertParsed.query,
            [(x[Types.cid], x[Types.publicationDate]) for x in success])
        dbConnector.executeMany(Statements.insertRecord.query,
                                [y for x in success for y in x["data"] ])
    dbConnector.commit()
    for text in texts:
        pipeEnd.send((Markers.TEXT, text[Types.cid]))
    pipeEnd.send(Markers.END)
        
class Middleman:
    """
    Middleman between the main execution, the Legifrance and the DB processes.
    
    Objects of this class SHOULD not be directly initialised; the static 
    create method should be used instead: when the object is fully initialised,
    it has already become useless.
    """
    def create(toLegi, toDb, fromCommand):
        """
        Blocking method creating a one-time Middleman.
        
        This method is blocking and will only return when the object has 
        completed its work (i.e. it has received the stand down order and has
        finished waiting for the results of its previous orders). 
        Thus, it
        SHOULD be used in a separate process unless all its orders, including
        the stand down one, have been
        sent beforehand to the appropriate connections.
        As the object is useless when the method returns, it is not returned.

        Parameters
        ----------
        toLegi : multiprocessing.connection.Connection
            This connection MUST be read/write. It will be used to send orders 
            to and receive results from the agent in charge of the Legifrance
            API.
            The easiest way to get such an object is to use one of the return 
            values of multiprocessing.Pipe(True).
        toDb : multiprocessing.connection.Connection
            This connection MUST be read/write. It will be used to send orders 
            to and receive results from the agent in charge of the interactions
            with the database.
            The easiest way to get such an object is to use one of the return 
            values of multiprocessing.Pipe(True).
        fromCommand : multiprocessing.connection.Connection
            This connection MUST be read/write. It will be used to receive
            orders from and send completion reports to the main agent.
            The easiest way to get such an object is to use one of the return 
            values of multiprocessing.Pipe(True).

        Returns
        -------
        None.

        """
        Middleman(toLegi, toDb, fromCommand)
        
    def __init__(self, toLegi, toDb, fromCommand):
        """
        Create an object transfering messages between Legifrance and the DB.
        
        While initialising, the object will listen to its three connections to
        get orders from the main agent and convert and transfer messages 
        between the agents respectively in charge of the interactions with 
        Legifrance and the database.
        When the initialisation process returns, the object has already 
        finished its job and has become useless.

        Parameters
        ----------
        toLegi : multiprocessing.connection.Connection
            This connection MUST be read/write. It will be used to send orders 
            to and receive results from the agent in charge of the Legifrance
            API.
            The easiest way to get such an object is to use one of the return 
            values of multiprocessing.Pipe(True).
        toDb : multiprocessing.connection.Connection
            This connection MUST be read/write. It will be used to send orders 
            to and receive results from the agent in charge of the interactions
            with the database.
            The easiest way to get such an object is to use one of the return 
            values of multiprocessing.Pipe(True).
        fromCommand : multiprocessing.connection.Connection
            This connection MUST be read/write. It will be used to receive
            orders from and send completion reports to the main agent.
            The easiest way to get such an object is to use one of the return 
            values of multiprocessing.Pipe(True).
        """
        self._commandsSet = {"wait"}
        self._commandsDict = dict()
        self._toLegi = toLegi
        self._toDb = toDb
        self._fromCommand = fromCommand
        while self._continue():
            connection.wait([toLegi, toDb, fromCommand])
            #Each method checks if its connection is ready
            self._handleOrder()
            self._handleLegiMsg()
            self._handleDbMsg()
        #All commands have been completed, send shutdown signal
        for pipe in [toLegi, toDb]:
            pipe.send(Markers.END)
        fromCommand.send(Markers.END)

    def _handleOrder(self):
        """React to messages received from the control connection."""
        while self._fromCommand.poll(0) and "wait" in self._commandsSet:
            message = self._fromCommand.recv()
            if message == Markers.END:
                self._commandsSet.remove("wait")
            elif message in SearchFilters:
                self._commandsSet.add(message)
                self._toLegi.send((Markers.TEXT_LIST, message))
    
    def _handleLegiMsg(self):
        """React to messages received from the Legifrance connection."""
        while self._toLegi.poll(0):
            message = self._toLegi.recv()
            if message[0] == Markers.END:
                #End of a TEXT_LIST command
                self._commandsSet.remove(message[1])
            elif message[0] == Markers.TEXT_LIST:
                #list of CIDs to check
                #Exclude the search filters, the DB does not need it
                self._toDb.send(message[:1] + message[2:])
                #Use CID of first element as key, search filter as value
                try:
                    self._commandsDict[message[2][0]] = message[1]
                except KeyError:
                    print(message)
                    raise
            elif message[0] == Markers.TEXT:
                #Text to parse
                #Do not remove criteria from commands yet: wait for storage
                criteria = self._commandsDict[message[1][Types.cid]]
                text = _parseText(message[1], criteria)
                self._toDb.send((Markers.TEXT, text))
            else:
                print("_handleLegiMsg not yet implemented: " + str(message))
    
    def _handleDbMsg(self):
        """React to messages received from the database connection."""
        while self._toDb.poll(0):
            message = self._toDb.recv()
            if message == Markers.END:
                #Message sent when _storeText returns, ignore: TEXT suffices.
                pass
            elif message[0] == Markers.TEXT_LIST:
                #Message is (TEXT_LIST, first queried CID, valid CIDs)
                #List of CIDs to request
                criteria = self._commandsDict.pop(message[1])
                if message[2]:
                    for cid in message[2]:
                        self._commandsDict[cid] = criteria
                    self._toLegi.send((Markers.TEXT, message[2]))
            elif message[0] == Markers.TEXT:
                #Message is (TEXT, cid of the text)
                self._commandsDict.pop(message[1])
            else:
                print("handleDbMsg not yet implemented: " + str(message))
        
    def _continue(self):
        """
        Check if the object MUST continue to wait for messages.
        
        The agent will stop waiting for messages once it has received the 
        shutdown signal and all the actions it has started have been completed.
        Thus, the agent MAY keep sending new messages to Legifrance and the 
        database after it has received the shutdown signal to complete ongoing
        tasks.

        Returns
        -------
        bool
            True if there are ongoing tasks and/or if the shutdown signal has
            not been received yet.

        """
        return bool(self._commandsSet) or bool(self._commandsDict)