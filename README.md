Plagerism-Checker
=================

Project for cs 831 class- Data mining 
Programming Assignment #1
Course         :     CS831 Data Mining
Semester       :     Summer-2014


Step 1: Exatract P1 contents under cloudera home directory in CDH  /home/clouder/P1/

eg:
[cloudera@localhost P1]$ pwd
/home/cloudera/P1

[cloudera@localhost P1]$ ls 
f1.txt  f2.txt  f3.txt  f4.txt  f5.txt  f6.txt  f7.txt  f8.txt  f9.txt  Plag_dir  Plag_dir.jar  Plagiarism.java  run.sh  t10.dat

Step 2: Place all input data file in HDFS (eg: /user/cloudera/input/)

eg:
hadoop fs -mkdir /user/cloudera/input
hadoop fs -put t10.dat /user/cloudera/input/
hadoop fs -put f*.txt /user/cloudera/input/

Step 3: Run Automated shell script under P1 folder /home/cloudera/P1/run.sh

[cloudera@localhost data]$ ls -lrt run.sh 
[cloudera@localhost data]$sh run.sh

Step 4: Check the result in

hadoop fs -cat /user/cloudera/output/part-r-00000

 hadoop fs -cat /user/cloudera/output/part-r-00000
                                    
f1.txt & f2.txt---> NO  (Not Simillar)
f1.txt & f3.txt---> Yes (Simillar)
f2.txt & f3.txt---> Yes (Simillar)
f1.txt & f4.txt---> Yes (Simillar)
f2.txt & f4.txt---> NO  (Not Simillar)
f3.txt & f4.txt---> Yes (Simillar)

