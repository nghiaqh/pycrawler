#!/usr/bin/python
import logging
import sys
import re
import urllib2, urllib
import httplib
import urlparse
import threading
import robotparser
import MySQLdb
from config import crawl_database as db, fileext

class PyCrawler( threading.Thread ):
    # Parser for robots.txt that helps determine if we are allowed to fetch a url
    rp = robotparser.RobotFileParser()
    crawled = []
    verbose = True
    timeout = 128 #seconds
    
    def run(self):
        # Connect to the db and create the tables if they don't already exist
        self.connection = MySQLdb.connect(db['host'], db['user'], db['passwd'], db['db'])
        self.cursor = self.connection.cursor()
        crawling = None
        try:
            self.cursor.execute("SELECT * FROM queue ORDER BY depth ASC, id ASC LIMIT 1")
            crawling = self.cursor.fetchone()
            self.cursor.execute("DELETE FROM queue WHERE id = %s" % crawling[0])
            self.connection.commit()
            if self.verbose:
                print crawling[2]
        except KeyError:
            raise StopIteration
        except Exception, e:
            print e
            pass
        # if theres nothing in the queue, then set the status to done and exit
        if crawling == None:
            self.cursor.execute("INSERT INTO status(s, t) VALUES(0, now())")
            self.connection.commit()
            print('Queue is empty. No remained links to crawl.')
        # Crawl the link
        else:
            self.crawl(crawling)
        return

    def crawl(self, crawling):
        depth = crawling[1]
        curl = crawling[2]
        parent = crawling[3]
        url = urlparse.urlparse(curl)

        try:
            # Have our robot parser grab the robots.txt file and read it
            self.rp.set_url('http://' + url[1] + '/robots.txt')
            self.rp.read()
            # If we're not allowed to open a url, return the function to skip it
            if not self.rp.can_fetch('PyCrawler', curl):
                if self.verbose:
                    print curl + " not allowed by robots.txt"
                query = "INSERT INTO crawl_index (url, parent, status) VALUES (\"%s\",\"%s\",\"%s\")" % (curl, parent, 403)
                self.cursor.execute(query)
                self.connection.commit()
        except:
            pass

        try:
            # Add the link to the already crawled list
            self.crawled.append(curl)
        except MemoryError:
            # If the crawled array is too big, deleted it and start over
            del self.crawled[:]
            
        try:
            if url.hostname == 'ndep.webdevdc.com' and (not (url.path.split('.')[-1] in fileext)):
#                request = urllib2.Request(curl)
#                request.add_header("User-Agent", "PyCrawler")
#                # Build the url opener, open the link and read it into msg
#                opener = urllib2.build_opener()
#                msg = opener.open(request).read()
                response = urllib2.urlopen(curl, None, self.timeout)
                msg = response.read()
                status = response.code
                reason = response.msg
                if msg is not None:
                    linkregex = re.compile('<a.*\shref=[\'"](.*?)[\'"].*?>')
                    links = linkregex.findall(msg)
                    print "Links: ", len(links)
                    self.queue_links(url, depth+1, links)
            else
                status, reason = self.checkURL(url)  
            print "Finished crawling: %d %s" % (status, reason)
        except Exception,e:
            print e
            status = 0

        try:
            query = "INSERT INTO crawl_index (url, parent, status) VALUES (\"%s\",\"%s\",\"%s\")" % (curl, parent, status)
            self.cursor.execute(query)
            self.connection.commit()
        except Exception,e:
            print e
            pass

    def queue_links(self, url, depth, links):
        # Read the links and inser them into the queue
        for link in links:
            if link.startswith('/'):
                link = refine_url('http://' + url[1] + link)
            elif link.startswith('#'):
                continue
            elif not link.startswith('http'):
                link = refine_url(urlparse.urljoin(url.geturl(),link))

            self.cursor.execute("SELECT url FROM crawl_index WHERE url=\"%s\"" % link)
            result = self.cursor.fetchall()
            self.cursor.execute("SELECT url FROM queue WHERE url=\"%s\""  % link)
            result2 = self.cursor.fetchall()

            if not (result or result2) and (link.decode('utf-8') not in self.crawled):
                try:
                    self.cursor.execute("INSERT INTO queue (url, depth, parent) VALUES(\"%s\",\"%s\",\"%s\")" % (link, depth, url.geturl()))
                    self.connection.commit()
                except Exception,e:
                    print e
                    continue

    def refine_url(self, url):
        return url.split('#')[0]
    
    def checkURL(self, url):
        conn = httplib.HTTPConnection(url[1])
        conn.request("HEAD", url[2])
        res = conn.getresponse()
        return int(res.status), res.reason

if __name__ == '__main__':
    connection = MySQLdb.connect(db['host'], db['user'], db['passwd'], db['db'])
    cursor = connection.cursor()
    cursor.execute("SELECT COUNT(id) FROM queue")
    number = cursor.fetchone()[0]
    if number==0:
        print "There's no urls in queue. Either setup.py hasn't been run or crawling was finished."
        return
        
    while number > 0:
        PyCrawler().run()
        cursor.execute("SELECT COUNT(id) FROM queue")
        number = cursor.fetchone()[0]
