# Steve Cosgrove 7 Jan 2024
# Testing recognition of serial port we can read from

import os

if not os.path.isfile('/dev/ttyS0'):
   # We have a Pi with built-in Wireless so use S0
   Serial_Port = '/dev/ttyS0'
else:
   # Go back to using AMA0
   Serial_Port = '/dev/ttyAMA0'

print("Using "+Serial_Port)
