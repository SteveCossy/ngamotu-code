#!/usr/bin/env python

# Adapted from Serial_to_MQTT by Andrew Hornblow
# by Steve Cosgrove 6 October 2023
# CSV function based on  https://github.com/SteveCossy/IOT/blob/master/LoRaReAd/MQTTUtils.py#L58

import datetime, time, serial, logging, os, csv
import paho.mqtt.client as mqtt
import json

# Constants for CSV handling
CSVPath     = '/home/pi/ngamotu'
CSV         = '.csv'
CSVFileBase = 'ngamotu_sensors_'
CrLf        = '\r\n'
FIELDNAMES  = ['time','channel','data']

# MQTT Params
MQTT_BROKER = 'penguin.econode.nz'
MQTT_PORT = 2883
MQTT_USER = 'penguin'
MQTT_PASSWORD = 'penguin2023'

# Default location of serial port on pre 3 Pi models
#SERIAL_PORT =  "/dev/ttyAMA0"

# Default location of serial port on Pi models 3 and Zero
SERIAL_PORT =   "/dev/ttyS0"

#This sets up the serial port specified above. baud rate is the bits per second timeout seconds
#port = serial.Serial(SERIAL_PORT, baudrate=2400, timeout=5)

#This sets up the serial port specified above. baud rate and WAITS for any cr/lf (new blob of data from picaxe)
port = serial.Serial(SERIAL_PORT, baudrate=2400)

#Connect to MQTT
client = mqtt.Client()
client.username_pw_set(MQTT_USER,MQTT_PASSWORD)
client.connect(MQTT_BROKER,MQTT_PORT)
client.loop_start()
qos = 10
timestamp = time.time()

while True:
  try:
    rcv = port.readline() #read buffer until cr/lf
    # print("Serial Readline Data = " + rcv)
    rcv = rcv.rstrip("\r\n")
    synch,node,channel,data,cs = rcv.split(",")
    print("rcv.split Data = : " + node + " " + channel + " " + data + " " + cs)
    #time.sleep(1)
    #Pacing delay to control rate of upload data

    checkSum = int(node) + int(channel) + int(data) %256
    cs = int(cs)
    #print(checkSum,cs)

    if checkSum != cs :
      qos = qos - 1
      # print("-1" , qos)
      if qos < 1 :
        qos = 1
    else :
      qos = qos + 1
      # print("+1" , qos)
      if qos > 100 :
        qos = 100
      CurrentTime = datetime.datetime.now().isoformat()
      CSVFile     = CSVFileBase+str(channel)+CSV
      CSVPathFile = os.path.join(CSVPath, CSVFile)

      DATALIST = {'time':CurrentTime,
                  'channel':channel,
                  'data':data
                  }
      print ( 'Save2CSV', DATALIST )
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



