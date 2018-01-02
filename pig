#### DATA LOADED FROM HDFS #####

data= LOAD '/home/hduser/Downloads/Drive/MapReduce/train_data.txt' USING PigStorage('|') AS (trno:int,trtime:chararray,trspeed:int,trdir:chararray);





5)If​ ​ you​ ​ receive​ ​ some​ ​ data​ ​ that​ ​ is​ ​ incorrect​ ​ in​ ​ input​ ​ file,​ ​ add additional​ ​ code​ ​ to​ ​ handle​

A= filter data by trno is not  null;



1)Total​ ​ how​ ​ many​ ​ train​ ​ data​ ​ in​ ​ present​ ​ in​ ​ the​ ​ input​ ​ data.


B= group A all;


C= foreach B  generate COUNT (A);



2)How​ ​ many​ ​ trains​ ​ are​ ​ travelling​ ​ from​ ​ this​ ​ station​ ​ in​ ​ each​ ​ hour


D = foreach A generate trno, SUBSTRING (trtime,1,3) as trtime:int,trspeed,trdir;

E =group D by trtime;


F= foreach E  generate group , COUNT (D);


3)How​ ​ many​ ​ trains​ ​ are​ ​ travelling​ ​ in​ ​ each​ ​ direction​ ​ in​ ​ each​ ​ hour



G =group D by (trtime,trdir);

H= foreach G  generate group , COUNT (D);

4)What​ ​ is​ ​ the​ ​ avg​ ​ speed​ ​ of​ ​ each​ ​ train​ ​ travelling​ ​ in​ ​ each​ ​ direction in​ ​ each​ ​ hour

I= group D by (trno,trtime,trdir);

J= foreach I  generate group , AVG (D.trspeed);
