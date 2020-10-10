# -*- coding: utf-8 -*-
"""
Provide base classes to use and extend to collect data from Legifrance.

This file MUST NOT be tailored to specific use cases.

Classes
-------
Bricks
    Base building blocks and utility methods used by the other classes.
Pattern
    Provide mechanisms to define a regex and use it to parse a text.
TextPatterns
    Abstract enum to subclass when defining the structure of a type of text.
"""

from enum import Enum
from dbStructure import Statements
import re

class Bricks(Enum):
    """
    Base building blocks and utility methods used by the other classes.
    """
    def repeat(pattern, capture=True):
        """
        Repeat one or more times a regex pattern.

        Parameters
        ----------
        pattern : str
            Pattern to repeat.
        capture : bool, optional
            If True, the pattern will be repeated in a capturing group, 
            otherwise in a non-capturing group. The default is True.

        Returns
        -------
        str
            The input pattern in a repeated group. For exemple, if pattern is
            "a", the output is "(a)+".
        """
        return (r"(" if capture else r"(?:") + pattern + r")+"
    
    def getItem(enum, index):
        """
        Get an element of an enum based on its index.

        Parameters
        ----------
        enum : Enum
            Enumeration in which the item will be selected.
        index : int
            Index of the object to select in the enum.

        Returns
        -------
        enum
            Object defined at the index-th position in enum. For example, 
            getItem(Color, 1) where Color defines values RED and BLUE will 
            return Color.BLUE.
        """
        return enum[list(enum.__members__)[index]]
    
    def flatten(parsed):
        """
        Transform nested lists of dicts into a single-level list of dict.
        
        Each element of the returned list is a dict containing the key-values
        of all the dict traversed in the nesting. For example, the input
        {"a":1,"b":[{"c":2, "d":3}, {"c":4, "d":5}]} will be transformed in 
        [{"a":1, "c":2, "d":3}, {"a":1, "c":4, "d":5}].

        Parameters
        ----------
        parsed : list or dict
            A dict (or a list of dicts) containing arbitrary keys associated 
            with arbitrary values and possibly keys associated with lists, 
            where each of those lists contain only dicts.

        Returns
        -------
        result : list
            Each element is a dict containing all the keys associated with 
            actual values (i.e. not those associated with lists) 
            encountered while traversing the nested dicts.
        """
        parsedList = [parsed] if isinstance(parsed, dict) else parsed
        result = []
        for dico in parsedList:
            #Sort keys between actual values and nested dicts
            listKeys = []
            standardKeys = []
            for key in dico:
                if isinstance(dico[key], list):
                    listKeys.append(key)
                else:
                    standardKeys.append(key)
            if not listKeys:
                #Terminal condition: no nested dict
                result.append(dico)
            else:
                partialResult = [{x:dico[x] for x in standardKeys}]
                for key in listKeys:
                    #Create a dict with the keys from partialResult and
                    #from the nested dicts
                    recurs = Bricks.flatten(dico[key])
                    partialResult = [{**x, **y} for x in partialResult for y in recurs]
                result.extend(partialResult)
        return result
    
    __baseNewline = r"(?:<p(?: align='\w*')?>|<br/>|</p>| )"
    _newline = __baseNewline + "+"
    _optionalNewline = __baseNewline + "*"
    _upperString = r"[A-ZÀÉÈÔ' \-]+"
    _romanNumber = r"(?:[IVX]+\. (?:\-|―) )"
    _ANString = r"[\w' \-/]+"
    _XString = r"[\w' \-,/\.]+"

class Pattern:
    """
    Provide mechanisms to define a regex and use it to parse a text.
    
    This class MUST be used to define the values in TextPattern subclasses. 
    It encapsulates a regex with a knowledge of its capturing groups and, 
    optionally, values to be ignored for each capturing group and possibly
    repeated nested subpattern.
    
    Methods
    -------
    match(self, text)
        Parse a text and return the captured values.
    """
    def __init__(self, regex, groups, nestedPattern = None, ignored = dict()):
        """
        Initalise a Pattern object.

        Parameters
        ----------
        regex : str
            Regular expression (as per the re module) where all the capturing 
            groups have been replaced with '{}'. Its definition MAY use values
            from the Bricks enum.
        groups : ordered collection of dbStructure.Types
            List of the non-repeated capturing groups in the regex. The 
            repeated capturing groups MUST be handled by a repeated nested 
            subpattern.
            The list MUST be in the same order as they appear in the final 
            regex: the placeholders will be replaced by the values from this 
            list.
        nestedPattern : Pattern, optional
            Pattern whose regex is encapsulated in this one. This offers the 
            capability to capture all the values for repeated capturing groups.
            The default is None.
        ignored : dict, optional
            Each key MUST appear in groups, and be associated with a collection
            of str. All matches where a capturing group captures a value in 
            this collection will be ignored. The default is dict().
        """
        self.regex = regex.format(*[x.group() for x in groups])
        self.groups = groups
        self.ignored = ignored
        self.nestedPattern = nestedPattern
        self.name = "_"
        while self.name in self.groups:
            self.name += "_"
        
    def _matchPart(self, part):
        """
        Recursively extract all informations from a text.

        Parameters
        ----------
        part : str
            Part of text to parse.
        Returns
        -------
        list
            A list containing, for each occurrence of the pattern in the text,
            a dictionary containing as keys each of the names captured by the
            pattern associated with their captured values, and an arbitrary key
            associated with the list returned by the method called on
            the occurrence of the pattern and its nested subpattern, if any.
            All records matching an ignored value are ignored and not included
            in the list.
        """
        return [{**{key.name:p[key.name] for key in self.groups},
                 **({#Call recursively on nested subpattern
                     self.name:self.nestedPattern._matchPart(
                     #and match
                     p[0])}
                     #only if subpattern exists
                    if self.nestedPattern is not None else {})}
                for p in re.finditer(self.regex, part)
                #discard any record in ignored
                if not any([p[key.name] in self.ignored[key]
                        for key in self.ignored])] 
    
    def match(self, text):
        """
        Extract all data captured by the pattern from the text.
        
        This method SHOULD be called on the root of a pattern chain.

        Parameters
        ----------
        text : str
            Text to parse.

        Returns
        -------
        list
            A list containing as elements dicts. Each dict describes a record
            from the parsed text: all the captured groups describing it, except
            those to ignore.
        """
        match = re.match(self.regex, text)
        if match is None:
            return None
        else:
            return Bricks.flatten(self._matchPart(match[0]))

class TextPattern(Enum):
    """
    Abstract enum to subclass when defining the structure of a type of text.
    
    Subclasses of this class MUST define a value called main that contains the 
    global pattern of the text. Subclasses of this class meant to capture data
    to send to the database MUST also define a static method called 
    prepareForInsertion(list, dict),
    building on this class' prepareForInsertion method to format the results 
    to execute the prepared statement inserting results in the database.
    """
    def __init__(self, pattern):
        """Initialise a text pattern encapsulating a Pattern object."""
        self.pattern = pattern
    
    def prepareForInsertion(parsed, values):
        """
        Format the parsing result for insertion in the database.

        Parameters
        ----------
        parsed : list of dict
            Results as returned by Pattern.match.
        values : dict
            Keys SHOULD be Types listed in Statements.insertRecord.args. The
            associated value will be included in each yielded result, along 
            with the values from the corresponding record. Missing values will
            be replaced with None.

        Yields
        ------
        result : list
            List in the order expected by Statements.insertRecord.
        """
        for element in parsed:
            result = []
            for arg in Statements.insertRecord.args:
                name = arg.name
                if arg in values:
                    result.append(values[arg])
                else:
                    value = element[name] if name in element else None
                    #Cast the value to the appropriate type when necessary
                    result.append(arg.cast(value) if arg.cast and value else 
                                  value)
            yield result
        
    @classmethod
    def match(cls, text):
        """
        Extract all data captured by the pattern from the text.

        Parameters
        ----------
        text : str
            Text to parse.

        Returns
        -------
        list
            A list containing as elements dicts. Each dict describes a record
            from the parsed text: all the captured groups describing it, except
            those to ignore.
        """
        return cls.main.pattern.match(text)