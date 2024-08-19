#
# Copyright (C) (2019) CEFCA
#
# VO Simple TAP library is free software: you can redistribute it and/or
# modify it under the terms of the GNU General Public License as
# published by the Free Software Foundation, either version 3 of the
# License, or (at your option) any later version.
#
# VO Simple TAP library is distributed in the hope that it will be
# useful, but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with jplusdatafeedback library. If not,
# see <http://www.gnu.org/licenses/>.
#

"""
Utility module to execute TAP queries.

@author: Javier Hernandez (CEFCA)
@version: 1.0

    #
    # EXAMPLE OF USE
    #
    import getpass
    user = raw_input("Username: ")
    password = getpass.getpass("Password: ")
    ser = ADQLService("https://archive.cefca.es/catalogues/vo/tap/jplus-dr1", user, password)  # Secured service
    # ser = ADQLService("https://archive.cefca.es/catalogues/vo/tap/jplus-edr")  # Constructor for Public services

    # Async examples
    r = ser.exec_async("SELECT * FROM jplus.TileImage", top=5)
    print(r)
    print(extract_votable_results(r))

    # Saving result to file
    ser.exec_async("SELECT * FROM jplus.TileImage", filename='/tmp/images.xml')

    # Saving result to file
    ser.exec_async("SELECT * FROM jplus.TileImage", filename='/tmp/images.xml')

    # Sync example
    r = ser.exec_sync("SELECT * FROM jplus.filter")
    print(r)  # VOTABLE xml value. Can be processed with 'astropy.vo.votable'
    print(extract_votable_results(r))  # List of tuples of python primitive values

"""
import requests
import time
import xml.etree.ElementTree as ET


def int_converter(value):
    if value:
        return int(value)


def float_converter(value):
    if value:
        return float(value)


def int_array_converter(value):
    vals = value.split(' ')
    return [int(v) if v != 'None' else None for v in vals]


def float_array_converter(value):
    vals = value.split(' ')
    return [float(v) if v != 'None' else None for v in vals]


class ADQLException(Exception):
    
    def __init__(self, reason):
        self.reason = reason

    def __str__(self, *args, **kwargs):
        return self.reason


class ADQLService(object):
    """Class to execute synchronous or asynchronous AQDL query in a service implementor""" 

    def __init__(self, url, user=None, pwd=None):
        """Constructor.
        
        Parameters
        ----------
            url: base URL of the TAP service
            user: optional user name if the service is secured
            pwd: optional user password if the service is secured
        """
        self.url = url
        self.url_sync = url + '/sync'
        self.url_async = url + '/async'
        self.user = user
        self.pwd = pwd

        self.session = requests.Session()
        # Login at Archive
        if user is not None and 'archive.cefca.es' in url:
            self.session.post("https://archive.cefca.es/catalogues/login",
                              data={'login': user, 'password': pwd, 'submit': 'Sign+In'})

    def download(self, url):
        from io import BytesIO 
        rs = self.session.get(url, auth=(self.user, self.pwd), verify=False)
        return BytesIO(rs.content)

    def exec_sync(self, query, top=None):
        """ Executes an ADQL query in synchronous mode. Result format is VOTABLE.
        
        Parameters
        ----------
            query: the ADQL query to execute
            top: integer
                the maximum number of rows to return
        
        Returns a list of tuples with the  values of the result rows.  
        """
        params = {'VERSION': '1.0', 'REQUEST': 'doQuery', 'LANG': 'ADQL',
                  'FORMAT': 'votable', 'QUERY': query}

        if top:
            params['MAXREC'] = top

        try:
            if self.user:
                rr = self.session.post(self.url_sync, data=params, auth=(self.user, self.pwd), verify=False)
            else:
                rr = self.session.post(self.url_sync, data=params, verify=False)
            rr.raise_for_status()
            result = rr.text
            if is_votable_error(result):
                raise ADQLException(extract_votable_error(result))

            return result
        except Exception as err:
            raise ADQLException(str(err))

    def exec_async(self, query, timeout=2000, top=None, owner=None, desc=None, filename=None,
                   queue=None, formattable=None, delete=False):
        """ Executes an ADQL query in asynchronous mode.
        
        Parameters
        ----------
            query: the ADQL query to execute
            timeout: maximum number of seconds to wait for the response
            top: integer
                the maximum number of rows to return
            owner: name of the query owner
            desc: description of the query
            filename: optional path to a file where we want the results
            queue: queue for the job
            formattable: optional format for the result, the default is 'VOTABLE'
            delete: bool
                Delete the result after retrieving the data. Defaults to False.
        
        Returns the query result as a VOTable xml text or None if 'filename' is given 
        """
        params = {'VERSION': '1.0', 'REQUEST': 'doQuery', 'LANG': 'ADQL',
                  'FORMAT': 'votable', 'QUERY': query}

        if top:
            params['MAXREC'] = top
        
        if owner:
            params['OWNER'] = owner

        if desc:
            params['RUNID'] = desc

        if queue:
            params['QUEUE'] = queue

        if formattable:
            params['FORMAT'] = formattable
        
        try:
            if self.user:
                rr = self.session.post(self.url_async, data=params, auth=(self.user, self.pwd), verify=False)
            else:
                rr = self.session.post(self.url_async, data=params, verify=False)
            
            rr.raise_for_status()
            job_url = rr.url

            if self.user:
                self.session.post(job_url + '/phase', data={'PHASE': 'RUN'}, auth=(self.user, self.pwd), verify=False)
            else:
                self.session.post(job_url + '/phase', data={'PHASE': 'RUN'}, verify=False)

            n = 0
            while n < timeout:
                if self.user:
                    rs = self.session.get(job_url + '/phase', auth=(self.user, self.pwd), verify=False)
                else:
                    rs = self.session.get(job_url + '/phase', verify=False)            
                
                rs.raise_for_status()
                status = rs.text.strip().upper()

                if status == 'ERROR':
                    if self.user:
                        rs = self.session.get(job_url + '/error', auth=(self.user, self.pwd), verify=False)
                    else:
                        rs = self.session.get(job_url + '/error', verify=False)

                    rs.raise_for_status()
                    raise ADQLException(extract_votable_error(rs.text))

                elif status == 'COMPLETED':
                    stream = filename is not None
                    if self.user:
                        rs = self.session.get(job_url + '/results/result', auth=(self.user, self.pwd),
                                              stream=stream, verify=False)
                    else:
                        rs = self.session.get(job_url + '/results/result', stream=stream, verify=False)

                    rs.raise_for_status()
                    if stream:
                        with open(filename, 'wb') as fd:
                            for chunk in rs.iter_content(5242880):  # chunks of 5 MB
                                fd.write(chunk)
                        if delete:
                            self.session.delete(job_url)

                        return None
                    else:
                        if delete:
                            self.session.delete(job_url)

                        return rs.text

                # Wait for the response
                if n < 10:                
                    time.sleep(2)
                    n += 2
                else:
                    time.sleep(5)
                    n += 5

        except ADQLException as ae:
            raise ae  
        except Exception as err:
            raise ADQLException(str(err))


def converter_for(dtype, arraysize="1"):
    if arraysize == "1" or arraysize == "":
        if dtype in ["short", "int", "long"]:
            return int_converter
        if dtype in ["float", "double"]:
            return float_converter
    else:
        if dtype in ["short", "int", "long"]:
            return int_array_converter

        if dtype in ["float", "double"]:
            return float_array_converter
    
    return str


def extract_votable_results(text):
    """
    Process a VOTABLE response to extract the result values.
    Returns a list of tuples with the  values of the result rows.
    """
    t = ET.fromstring(text)
    pos = t.tag.find('}') + 1
    if pos != 0:
        ns = t.tag[:pos]
    else:
        ns = ""

    results = t.find("./{ns}RESOURCE[@type='results']".format(ns=ns))
    if results is None:
        results = t.find("./{ns}RESOURCE".format(ns=ns))
        if results is None:
            raise ADQLException("Invalid VOTABLE response. No results.")
    
    tbl = results.find("./{ns}TABLE".format(ns=ns))
    if tbl is None:
        raise ADQLException("Invalid VOTABLE response. No table.")

    try:
        fields = tbl.findall("./{ns}FIELD".format(ns=ns))
        converters = [converter_for(f.attrib['datatype'], f.attrib.get('arraysize', '')) for f in fields]
        rows = tbl.findall("./{ns}DATA/{ns}TABLEDATA/{ns}TR".format(ns=ns))
        result = [[converters[i]((row.findall("./{ns}TD".format(ns=ns)))[i].text) for i in range(len(converters))]
                  for row in rows]
    except Exception as err:
        raise ADQLException(str(err))

    return result


def is_votable_error(text):
    t = ET.fromstring(text)
    pos = t.tag.find('}') + 1
    if pos != 0:
        ns = t.tag[:pos]
    else:
        ns = ""

    results = t.find("./{ns}RESOURCE[@type='results']".format(ns=ns))
    if results is None:
        return False
    
    qe = results.find("./{ns}INFO[@name='QUERY_STATUS']".format(ns=ns))
    if qe is None:
        return False

    return qe.attrib['value'] == 'ERROR'
    

def extract_votable_error(text):
    t = ET.fromstring(text)
    pos = t.tag.find('}') + 1
    if pos != 0:
        ns = t.tag[:pos]
    else:
        ns = ""

    results = t.find("./{ns}RESOURCE[@type='results']".format(ns=ns))
    if results is None:
        raise ADQLException("Invalid VOTABLE response. No results.")
    
    qe = results.find("./{ns}INFO[@name='QUERY_STATUS']".format(ns=ns))
    if qe is None:
        raise ADQLException("Invalid VOTABLE response. No query status.")

    return qe.text


if __name__ == '__main__':
    # 
    # THIS ARE JUST EXAMPLES
    #

    # ser = ADQLService("http://archive.cefca.es/catalogues/vo/tap/jplus-edr")  # Constructor for Public services

    import getpass
    user = input("Username: ")
    password = getpass.getpass("Password: ")
    ser = ADQLService("http://archive.cefca.es/catalogues/vo/tap/jplus-dr1", user, password)  # Secured service
    
    # Sync example
    r = ser.exec_sync("SELECT * FROM filter")
    print(extract_votable_results(r))  # List of tuples of python primitive values
    r = ser.exec_sync("SELECT TOP 2 0, class_star, mag_auto, flags[5], flags FROM jplus.magabdualobj")
    # print(r) # VOTABLE xml value. Can be processed with 'astropy.vo.votable'
    print(extract_votable_results(r))  # List of tuples of python primitive values

    try: 
        r = ser.exec_sync("SELECT * FROMjplus.TileImage", top=5)
        print(extract_votable_results(r))
    except ADQLException as e:
        print(e)
    
    # Async examples 
    r = ser.exec_async("SELECT * FROM jplus.TileImage", top=5)
    # print r # VOTABLE xml value. Can be processed with 'astropy.vo.votable'
    print(extract_votable_results(r))
    
    # Saving result to file
    ser.exec_async("SELECT * FROM jplus.TileImage", filename='/tmp/images.xml', delete=True)

    try: 
        r = ser.exec_async("SELECT * FROMjplus.TileImage", top=5)
    except ADQLException as e:
        print(e)
