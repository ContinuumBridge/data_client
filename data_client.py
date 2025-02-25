#!/usr/bin/env python
# data_client.py
# Copyright (C) ContinuumBridge Limited, 2017 - All Rights Reserved
# Unauthorized copying of this file, via any medium is strictly prohibited
# Proprietary and confidential
# Written by Peter Claydon & Martin Sotheran
#
"""
Just stick actions from incoming requests into threads.
"""

import json
import requests
import time
import sys
import os.path
import signal
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.MIMEImage import MIMEImage
from subprocess import check_output
import logging
import logging.handlers
import twilio
import twilio.rest
from autobahn.twisted.websocket import WebSocketClientFactory, WebSocketClientProtocol, connectWS
from twisted.internet import threads
from twisted.internet import reactor, defer
from twisted.internet.protocol import ReconnectingClientFactory
from influxdb import InfluxDBClient

config                = {}
#HOME                  = os.path.expanduser("~")
HOME                  = os.getcwd()
CB_ADDRESS            = "portal.continuumbridge.com"
DBURL                 = "http://onepointtwentyone-horsebrokedown-1.c.influxdb.com:8086/"
CB_LOGGING_LEVEL      = "DEBUG"
CB_LOGFILE            = HOME + "/data_client.log"
CONFIG_FILE           = HOME + "/data_client.config"
CONFIG_READ_INTERVAL  = 10.0

 
CB_LOGGING_LEVEL      = "DEBUG"
logger = logging.getLogger('Logger')
logger.setLevel(CB_LOGGING_LEVEL)
handler = logging.handlers.RotatingFileHandler(CB_LOGFILE, maxBytes=10000000, backupCount=3)
formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

try:
    s = check_output(["git", "status"])
    if "master" in s:
        CONFIG_FILE = HOME + "/data_client.config"
    else:
        CONFIG_FILE = HOME + "/staging.config"
    logger.info("Using config: {}".format(CONFIG_FILE))
    print("Using config: {}".format(CONFIG_FILE))
except Exception as e:
    logger.error("Problem setting config file: {}, {}".format(type(e), e.args))
    print("Problem setting config file: {}, {}".format(type(e), e.args))
    sys.exit()

reactor.suggestThreadPoolSize(30)

def nicetime(timeStamp):
    localtime = time.localtime(timeStamp)
    milliseconds = '%03d' % int((timeStamp - int(timeStamp)) * 1000)
    now = time.strftime('%H:%M:%S, %d-%m-%Y', localtime)
    return now

print "Start time: %s", nicetime(time.time())

def sendMail(to, sender, subject, body):
    try:
        user = config["mail"]["user"]
        password = config["mail"]["password"]
        # Create message container - the correct MIME type is multipart/alternative.
        msg = MIMEMultipart('alternative')
        msg['Subject'] = subject
        msg['From'] = sender + config["mail"]["from"]
        recipients = to.split(',')
        [p.strip(' ') for p in recipients]
        if len(recipients) == 1:
            msg['To'] = to
        else:
            msg['To'] = ", ".join(recipients)
        # Create the body of the message (a plain-text and an HTML version).
        text = body + " \n"
        htmlText = text
        # Record the MIME types of both parts - text/plain and text/html.
        part1 = MIMEText(text, 'plain')
        part2 = MIMEText(htmlText, 'html')
        msg.attach(part1)
        msg.attach(part2)
        mail = smtplib.SMTP('smtp.gmail.com', 587)
        mail.ehlo()
        mail.starttls()
        mail.login(user, password)
        mail.sendmail(user, recipients, msg.as_string())
        logger.debug("Sent mail")
        mail.quit()
    except Exception as ex:
        logger.warning("sendMail problem. To: %s, type %s, exception: %s", to, type(ex), str(ex.args))

def postInfluxDB1(dat, bid, db):
    """
    global prevDat
    try:
        if dat == prevDat:
            logger.warning("Posting duplicate data old: %s", json.dumps(prevDat,indent=4))
            logger.warning("Posting duplicate data new: %s", json.dumps(dat,indent=4))
    except:
        logger.warning("First time round")

    prevDat = dat
    """
    url = config["dburl2"]
    #logger.debug("InfluxDB1, url: {}".format(url))
    #logger.debug("Influx, writing to db %s", db)
    #logger.debug("InfluxDB1, writing: {}\n to database %s".format(dat), db)
    logger.debug("Posting to %s database: %s", db, json.dumps(dat, indent=4))
    try:
        client = InfluxDBClient(host=url, port=8086, database=db)
        result = client.write_points(dat, time_precision="ms")
    except Exception as e:
        logger.warning("postInfluxApi, api problem: {} {}".format(type(e), e.args))

def postInfluxDB(dat, bid):
    try:
        if "database" in config["bridges"][bid]:
            url = config["dburl"] + "db/" + config["bridges"][bid]["database"] + "/series?u=root&p=" + config["dbrootp"]
        else:
            url = config["dburl"] + "db/Bridges/series?u=root&p=" + config["dbrootp"]
        headers = {'Content-Type': 'application/json'}
        status = 0
        logger.debug("url: %s", url)
        """
        r = requests.post(url, data=json.dumps(dat), headers=headers)
        status = r.status_code
        if status !=200:
            logger.warning("POSTing failed, status: %s", status)
        """
    except Exception as ex:
        logger.warning("postInfluxDB problem, type %s, exception: %s", type(ex), str(ex.args))

def doPumpco(body, bid):
    logger.debug("doPumpco")
    try:
        dat = {
            "continuum_bridge": {
                "body": body
            }
        }
        logger.debug("Sending data: " + json.dumps(dat, indent=4))
        url = config["service_providers"]["pumpco"]["url"] + bid + "/measurement.json"
        logger.debug("Sending to url: %s", url)
        headers = {'Content-Type': 'application/json'}
        status = 0
        logger.debug("url: %s", url)
        r = requests.post(url, data=json.dumps(dat), headers=headers)
        status = r.status_code
        if status !=200:
            logger.warning("POSTing failed, status: %s", status)
    except Exception as ex:
        logger.warning("postInfluxDB problem, type %s, exception: %s", to, type(ex), str(ex.args))

def sendSMS(messageBody, to):
    numbers = to.split(",")
    for n in numbers:
       try:
           client = twilio.rest.TwilioRestClient(config["twilio_account_sid"], config["twilio_auth_token"])
           message = client.messages.create(
               body = messageBody,
               to = n,
               from_ = config["twilio_phone_number"]
           )
           sid = message.sid
           logger.debug("Sent sms: %s", str(n))
       except Exception as ex:
           logger.warning("sendSMS, unable to send message %s to: %s, type %s, exception: %s", messageBody, str(to), type(ex), str(ex.args))

def authorise():
    if True:
    #try:
        auth_url = "http://" + CB_ADDRESS + "/api/client/v1/client_auth/login/"
        auth_data = '{"key": "' + config["cid_key"] + '"}'
        auth_headers = {'content-type': 'application/json'}
        response = requests.post(auth_url, data=auth_data, headers=auth_headers)
        cbid = json.loads(response.text)['cbid']
        if response.status_code != requests.codes.ok:
            logger.info("Got bad server response: %s", response)
            exit()
        else:
            logger.info("Got good server response: %s ", str(response))
        cbid = json.loads(response.text)['cbid']
        logger.info("Got cbid= "+ json.dumps(cbid, indent=4))
        sessionID = response.cookies['sessionid']
        ws_url = "ws://" + CB_ADDRESS + ":7522/"
        return cbid, sessionID, ws_url
    #except Exception as ex:
    #    logger.warning("Unable to authorise with server, type: %s, exception: %s", str(type(ex)), str(ex.args))
    
def readConfig(forceRead=False):
    #logger.debug("readConfig")
    try:
        global config
        oldconfig = config
        if time.time() - os.path.getmtime(CONFIG_FILE) < 600 or forceRead:
            with open(CONFIG_FILE, 'r') as f:
                newConfig = json.load(f)
                config.update(newConfig)
            for c in config:
                if c.lower in ("true", "t", "1"):
                    config[c] = True
                elif c.lower in ("false", "f", "0"):
                    config[c] = False
            #logger.info("Read new config: " + json.dumps(config, indent=4))
            if config != oldconfig:
                logger.info("Config changed")
                return True
            else:
                return False
    except Exception as ex:
        logger.warning("Problem reading config file, type: %s, exception: %s", str(type(ex)), str(ex.args))
        return False

def readConfigLoop():
    readConfig(True)
    reactor.callLater(CONFIG_READ_INTERVAL, readConfigLoop)

class ClientWSFactory(ReconnectingClientFactory, WebSocketClientFactory):
    maxDelay = 60
    maxRetries = 200
    def startedConnecting(self, connector):
        logger.debug('Started to connect.')
        ReconnectingClientFactory.resetDelay

    def clientConnectionLost(self, connector, reason):
        logger.debug('Lost connection. Reason: %s', reason)
        ReconnectingClientFactory.clientConnectionLost(self, connector, reason)

    def clientConnectionFailed(self, connector, reason):
        logger.debug('Lost reason. Reason: %s', reason)
        ReconnectingClientFactory.clientConnectionFailed(self, connector, reason)

class ClientWSProtocol(WebSocketClientProtocol):
    def __init__(self):
        logger.debug("Connection __init__")
        signal.signal(signal.SIGINT, self.signalHandler)  # For catching SIGINT
        signal.signal(signal.SIGTERM, self.signalHandler)  # For catching SIGTERM
        self.stopping = False

    def signalHandler(self, signal, frame):
        logger.debug("signalHandler received signal")
        self.stopping = True
        reactor.stop()

    def sendToBridge(self, message):
        self.sendMessage(json.dumps(message))

    def onConnect(self, response):
        logger.debug("Server connected: %s", str(response.peer))

    def onOpen(self):
        logger.debug("WebSocket connection open.")

    def onClose(self, wasClean, code, reason):
        logger.debug("onClose, reason:: %s", reason)

    def onMessage(self, message, isBinary):
        #logger.debug("onMessage")
        try:
            msg = json.loads(message)
            logger.info("Message received: %s", json.dumps(msg, indent=4))
        except Exception as ex:
            logger.warning("onmessage. Unable to load json, type: %s, exception: %s", str(type(ex)), str(ex.args))
        if not "source" in msg:
            logger.warning("onMessage. message without source")
            return
        if not "body" in msg:
            logger.warning("onMessage. message without body")
            return
        if msg["body"] == "connected":
            logger.info("Connected to ContinuumBridge")
        else:
            bid = msg["source"].split("/")[0]
            aid = msg["source"].split("/")[1]
            #logger.debug("bid: %s", bid)
            #logger.debug("config_bridges: %s", str(config["bridges"]))
            if bid not in config["bridges"]:
                logger.info("Message from unregistered bridge: %s", bid)
                return
            found = False
            for body in msg["body"]:
                rx_n = 0
                if "n" in body:
                    found = True
                    if rx_n <= body["n"]:
                        rx_n = body["n"]
                        del body["n"]
                self.processBody(msg["source"], body, bid, aid)
            if found:
                ack = {
                        "source": config["cid"],
                        "destination": msg["source"],
                        "body": [
                                    {"a": rx_n}
                                ]
                      }
                #logger.debug("onMessage ack: %s", str(json.dumps(ack, indent=4)))
                reactor.callInThread(self.sendToBridge, ack)

    def processBody(self, destination, body, bid, aid):
        logger.debug("body: %s", str(body))
        if body["m"] == "alert":
            try:
                bridge = config["bridges"][bid]["friendly_name"]
                if "a" in body:
                    emailBody =  "Message from " + bridge + ": " + body["a"]
                    subject = body["a"]
                if "email" in config["bridges"][bid]:
                    reactor.callInThread(sendMail, config["bridges"][bid]["email"], bridge, subject, emailBody)
                if "sms" in config["bridges"][bid]:
                    reactor.callInThread(sendSMS, emailBody, config["bridges"][bid]["sms"])
            except Exception as ex:
                logger.warning("Problem processing alert message, exception: %s %s", str(type(ex)), str(ex.args))
        elif body["m"] == "data":
            do = {}
            do['fields'] = {}
            do['tags'] = {}
            logger.info("Data messsage received")
            if "service_provider" in config["bridges"][bid]:
                if config["bridges"][bid]["service_provider"] == "pumpco":
                    doPumpco(body, bid)
            else:
                try:
                #if True:
                    dat = body["d"]
                    if len(dat)>2:
                        logger.debug("Long d: %s", str(json.dumps(dat, indent=4)))
                    for d in dat:
                        logger.debug("New d: %s", str(json.dumps(d, indent=4)))
                        s = d["name"].split("/")                     
                        tdiff =  time.time() - d["points"][0][0]/1000
                        if abs(tdiff) > 300:
                            logger.warning("Bridge %s and client time are different by %s seconds", bid, str(tdiff))
                            logger.warning("    Bridge timestamp in %s: %s", d["name"], nicetime(d["points"][0][0]/1000))
                        do["time"] = d["points"][0][0]
                        if len(s) == 3:
                            do["tags"]["characteristic"] = s[2]
                            if s[2] == "power" or s[2] == "temperature":
                                do["fields"]["fvalue"] = float(d["points"][0][1])
                                do["fields"]["value"] = ""
                                logger.debug("Casting %s to  %s", d["points"][0][1], do["fields"]["fvalue"])
                            else:
                                do["fields"]["value"] = d["points"][0][1]
                                do["fields"]["fvalue"] = ""
                        else:
                            do["tags"]["characteristic"] = "event"
                            do["fields"]["value"] = d["points"][0][1]
                        do["measurement"] = s[0]
                        do["tags"]["sensor"] = s[1]
                        #if "friendly_name" in config["bridges"][bid]:
                        #    do["tags"]["friendly_name"] = config["bridges"][bid]["friendly_name"]
                        if "name_in_database" in config["bridges"][bid]:
                            do["tags"]["name_in_database"] = config["bridges"][bid]["name_in_database"]
                        dd = [do]
                        #N.B. it'll fail if the db doesn't exist - should create it here really
                        if "database" in config["bridges"][bid]:
                            db = config["bridges"][bid]["database"] 
                        else:
                            db = "Bridges"
                        reactor.callInThread(postInfluxDB1, dd, bid, db)
                except Exception as ex:
                    logger.warning("Problem processing data message, exception: %s %s", str(type(ex)), str(ex.args))
        elif body["m"] == "req_config":
            if "config" in config["bridges"][bid]:
                if aid in config["bridges"][bid]["config"]:
                    app_config = config["bridges"][bid]["config"][aid]
                else:
                    app_config = {"warning": "no config"}
                ack = {
                    "source": config["cid"],
                    "destination": destination,
                    "body": [
                        {"config": app_config}
                    ]
                }
                logger.debug("onMessage ack: %s", str(json.dumps(ack, indent=4)))
                reactor.callInThread(self.sendToBridge, ack)
            else:
                logger.info("onMessage. No config for: %s", bid)

if __name__ == '__main__':
    readConfig(True)
    cbid, sessionID, ws_url = authorise()
    headers = {'sessionID': sessionID}
    factory = ClientWSFactory(ws_url, headers=headers)
    factory.protocol = ClientWSProtocol
    connectWS(factory)
    reactor.callLater(CONFIG_READ_INTERVAL, readConfigLoop)
    reactor.run()
