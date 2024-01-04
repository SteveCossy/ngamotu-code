# Send the current files to GitHub
# Steve Cosgrove adapted on 6 October 2023

for DIR in /home/pi/ngamotu/
do
   cd $DIR
   git pull
   git add .
   git commit -m "`date +%F_%T`"
   git push
done

