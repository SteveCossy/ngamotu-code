#!/usr/bin/env python

# Adapted from Serial_to_MQTT by Andrew Hornblow
# by Steve Cosgrove 6 October 2023
# CSV function based on  https://github.com/SteveCossy/IOT/blob/master/LoRaReAd/MQTTUtils.py#L58

import datetime, time, serial, logging, os, csv, toml
import paho.mqtt.client as mqtt
import json

# Constants for CSV handling
CSVPath     = '/home/pi/ngamotu-data'
CSV         = '.csv'
CSVFileBase = 'ngamotu_sensors_'
CrLf        = '\r\n'
FIELDNAMES  = ['time','channel','data']
HOME_DIR    = os.environ['HOME']
# HOME_DIR  =    '/home/pi'
AUTH_FILE   = 'MQTTdetails.txt'
ConfPathFile = os.path.join(HOME_DIR, AUTH_FILE)

# MQTT Params read from AUTH_FILE defined above
# The format of the file at ConfPathFile needs to be:
# [econode]
# Broker = '<Broker URL>'
# Port = <Broker port number>
# User = '<Broker username>'
# Password = '<Broker password>'
# https://toml.io/en/

ConfigDict = toml.load(ConfPathFile)
MQTTparam = ConfigDict.get('econode')
# print (MQTTparam)

MQTT_BROKER   = MQTTparam['Broker']
MQTT_PORT     = MQTTparam['Port']
MQTT_USER     = MQTTparam['User']
MQTT_PASSWORD = MQTTparam['Password']

print ( \
   "Broker:", MQTT_BROKER, \
   "Port:",   MQTT_PORT, \
   "User:",   MQTT_USER, \
   )


# We have a Pi with built-in Wireless so use S0
# Default location of serial port on Pi models 3 and Zero W
SERIAL_PORT = '/dev/ttyS0'

# Go back to using AMA0
# Default location of serial port on non wireless models (Original Zero and pre 3 Pi models)
#   SERIAL_PORT = '/dev/ttyAMA0'

print ("Using "+SERIAL_PORT)

#This sets up the serial port specified above. baud rate  and no timeout
port = serial.Serial(SERIAL_PORT, baudrate=2400)

#Connect to MQTT
client = mqtt.Client()
client.username_pw_set(MQTT_USER,MQTT_PASSWORD)

try:
   CurrentTime = datetime.datetime.now().isoformat()
   print ('Connecting:' , CurrentTime )
   client.connect(MQTT_BROKER,MQTT_PORT)
except:
   print ('Connect failed.')

client.loop_start()
qos = 10
timestamp = time.time()
error = 0 #have not had any errors yet

while True:
  try:
    rcv = port.readline() #read buffer until cr/lf
#    print("Serial Readline Data  = ", rcv)

#   These lines were required to use with 6 Jan 2024 version of test generator
#    rcv = str(rcv[:-3]) # This doesn't work.  I'd like to know why!
    rcv = str(rcv[:-2])
    rcv = rcv[:-1]

#    rcv = rcv.rstrip("\r\n")  # Previous line used on site 6 Jan 2023 (Python 2)

    print("Serial String = ", rcv)

    synch,node,channel,data,cs = rcv.split(",")
#    print("rcv.split Data = : " + node + " " + channel + " " + data + " " + cs)
    #time.sleep(1)
    #Pacing delay to control rate of upload data

#    checkSum = int(node) + int(channel) + int(data) %256 # changed 17 Jan 24 at Andrew's request
    checkSum = int(node) + int(channel) + int(data)
    cs = int(cs)
    #print(checkSum,cs)

    if checkSum != cs :
      qos = qos - 1
      print('Checksum (' + str(checkSum) + ') Failed. QOS = ' , qos)
      if qos < 1 :
        qos = 1
    else :
      qos = qos + 1
 #     print('Checksum Passed! QOS = ' , qos)
      if qos > 100 :
        qos = 100
      CurrentTime = datetime.datetime.now().isoformat()
      CSVFile     = CSVFileBase+str(channel)+CSV
      CSVPathFile = os.path.join(CSVPath, CSVFile)

      DATALIST = {'time':CurrentTime,
                  'channel':channel,
                  'data':data
                  }
#      print ( 'Save2CSV', DATALIST )
      json_data = json.dumps( DATALIST, separators=(',', ':') )
      client.publish('penguin/gusto-raw/'+channel, json_data )

      if not os.path.isfile(CSVPathFile):
      # There is not currently an output file
        print ("Creating new output file: "+CSVPathFile)
        if not os.path.exists(CSVPath):
            os.mkdir(CSVPath)
        with open(CSVPathFile, 'w') as CSVFile:
            writer = csv.DictWriter(CSVFile, fieldnames=FIELDNAMES)
            writer.writeheader()
      with open(CSVPathFile, 'a') as CSVFile:
        writer = csv.DictWriter(CSVFile, fieldnames=FIELDNAMES)
        writer.writerow(DATALIST)



    if (time.time() > timestamp + 600):
      timestamp = time.time()
      channel   = 25
      data      = qos
      CurrentTime = datetime.datetime.now().isoformat()
      CSVFile     = CSVFileBase+str(channel)+CSV
      CSVPathFile = os.path.join(CSVPath, CSVFile)

      DATALIST = {'time':CurrentTime,
                  'channel':channel,
                  'data':data
                  }
      print ( 'Save2CSV', DATALIST )
      if not os.path.isfile(CSVPathFile):
      # There is not currently an output file
        print ("Creating new output file: "+CSVPathFile)
        if not os.path.exists(CSVPath):
            os.mkdir(CSVPath)
        with open(CSVPathFile, 'w') as CSVFile:
            writer = csv.DictWriter(CSVFile, fieldnames=FIELDNAMES)
            writer.writeheader()
      with open(CSVPathFile, 'a') as CSVFile:
        writer = csv.DictWriter(CSVFile, fieldnames=FIELDNAMES)
        writer.writerow(DATALIST)

  except ValueError:
    error = error + 10
    #error = float(error)/1
    #client.virtualWrite(22,error)/1
    #if Data Packet corrupt or malformed then...
    print("Data Packet corrupt or malformed")
