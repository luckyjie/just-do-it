#-*-coding:utf_8 -*-
'''
created Date:20161012
Author��wangjin
data1=sqlContext.sql("select sum(case when Date='null'then 1 else 0 end)as DateNULL,sum(case when Date!='null' then 1 else 0 end)as Date1 from dataTrain")
data2=sqlContext.sql("select count(*) from dataTrain where Date_received='null'")
data11=dataTrain1.map(lambda x:(int(x[0]),int(x[1]),int(x[3]),float(x[4]),int(x[5]),int(ProcesszType(x[6])),int(x[7]),int(ProcessType(x[8])))) 

'''
from sklearn.linear_model import LogisticRegression 
import numpy as np
from pyspark.sql import SQLContext,Row
import datetime,time
import time
def timeToDatetime(timeNumber,timeEnd):
    timestr = str(timeNumber)
    if(timeEnd==-1):
        return 0
    timestr0 = str(timeEnd)
    timestart = datetime.datetime.strptime(timestr[:4]+'-'+timestr[4:6]+'-'+timestr[6:], '%Y-%m-%d').date()
    timeend   = datetime.datetime.strptime(timestr0[:4]+'-'+timestr0[4:6]+'-'+timestr0[6:], '%Y-%m-%d').date()
    timeStart=timestart+datetime.timedelta(days=15)
    if(timeStart>=timeend):
        return 1
    else:
        return 0;
def ProcessType(stringName):
    print stringName 
    if stringName=='null':
        return -1;
    else:
        return int(stringName)
def  ProcessTrain(sc):
    sqlC=SQLContext(sc)
    dataTrain=sqlC.read.parquet('/hadoop/hadoop_/wangjin/ccf_offline_train.parquet')
    sqlC.registerDataFrameAsTable(dataTrain,"dataTrain")
    dataTrain1=sqlC.sql("select * from dataTrain where Date_received<'20160601' and Date_received!='null' and (Date<'20160601' or Date='null')")#�޸��·�
    sqlC.registerDataFrameAsTable(dataTrain1,"dataTrain1")
    datacount=sqlC.sql("select count(*) from dataTrain1").map(lambda x:int(x[0])).take(1)[0]
    dataTrain2=dataTrain1.map(lambda x:(int(x[0]),int(x[1]),int(x[2]),float(x[3]),int(x[4]),int(ProcessType(x[5])),int(x[6]),int(ProcessType(x[7]))))
    dataTrain2=dataTrain2.map(lambda x:(x[0],x[1],x[2],x[3],x[4],x[5],x[6],x[7],timeToDatetime(x[6],x[7])))
    #dataOfflineName=Row('user_id','Merchant_id','Coupon_id','Discount_rate0','Discount_rate1','Distance','Date_received','Date','label')
    #dataTrain2=dataTrain2.map(lambda x:dataOfflineName(*x))
    dataTrainX=np.array(dataTrain2.map(lambda x:(x[0],x[1],x[2],x[3],x[4],x[5],x[6])).take(datacount))# Date:x[7]
    dataTrainY=np.array(dataTrain2.map(lambda x:(x[8])).take(datacount)) 
    return dataTrainX,dataTrainY
def ProcessTest(sc):
    sqlC=SQLContext(sc)
    dataTest=sqlC.read.parquet('/hadoop/hadoop_/wangjin/ccf_offline_train.parquet')
    sqlContext.registerDataFrameAsTable(dataTest,"dataTest")
    dataTest1=sqlContext.sql("select * from dataTest where Date_received>='20160601' and Date_received!='null' and (Date>='20160601' or Date='null')")
    sqlC.registerDataFrameAsTable(dataTest1,"dataTest1")
    #print "123..."
    datacount=sqlC.sql("select count(*) from dataTest1").map(lambda x:int(x[0])).take(1)[0]
    dataTest2=dataTest1.map(lambda x:(int(x[0]),int(x[1]),int(x[2]),float(x[3]),int(x[4]),int(ProcessType(x[5])),int(x[6]),int(ProcessType(x[7]))))
    dataTest2=dataTest2.map(lambda x:(x[0],x[1],x[2],x[3],x[4],x[5],x[6],x[7],timeToDatetime(x[6],x[7])))
    #dataOfflineName=Row('user_id','Merchant_id','Coupon_id','Discount_rate0','Discount_rate1','Distance','Date_received','Date','label')
    #dataTest2=dataTest2.map(lambda x:dataOfflineName(*x))
    dataTestX=np.array(dataTest2.map(lambda x:(x[0],x[1],x[2],x[3],x[4],x[5],x[6])).take(datacount)) #Date :x[7]
    dataTestY=np.array(dataTest2.map(lambda x:(x[8])).take(datacount)) 
    return dataTestX,dataTestY
def ProcessRealTest(sc):
    sqlC=SQLContext(sc)
    dataTest=sqlC.read.parquet('/hadoop/hadoop_/wangjin/ccf_offline_test.parquet')
    sqlC.registerDataFrameAsTable(dataTest,"dataTest")
    dataTest1=sqlC.sql("select * from dataTest where Date_received>='20160701' ")
    sqlC.registerDataFrameAsTable(dataTest1,"dataTest1")
    #print "123..."
    datacount=sqlC.sql("select count(*) from dataTest1").map(lambda x:int(x[0])).take(1)[0]
    dataTest2=dataTest1.map(lambda x:(int(x[0]),int(x[1]),int(x[2]),float(x[3]),int(x[4]),int(ProcessType(x[5])),int(x[6])))
    #dataOfflineName=Row('user_id','Merchant_id','Coupon_id','Discount_rate0','Discount_rate1','Distance','Date_received')
    #dataTest2=dataTest2.map(lambda x:dataOfflineName(*x))
    dataTestRealX=np.array(dataTest2.map(lambda x:(x[0],x[1],x[2],x[3],x[4],x[5],x[6])).take(datacount))
    return dataTestRealX
def wPretxt(ArrayData,Num,name):
    fr=open('/data/home/hadoop/wangjin/'+name+str(Num)+'.txt','a')
    #fr1=open('/data/home/hadoop/wangjin/test.txt','a')
    for i in range(len(ArrayData)):
        fr.write(str(ArrayData[i])+'\n')
    fr.close()
def PowError(proY,TestY):
    sum1=(proY-TestY)*(proY-TestY)
    return sum(sum1)
def LRTest(sc,tol=1e-1):
    t1=time.time()
    dataTrainX,dataTrainY = ProcessTrain(sc)
    dataTestX,dataTestY = ProcessTest(sc)
    #dataTestRealX=ProcessRealTest(sc)
    model=LogisticRegression(penalty='l2', tol=tol)
    model.fit(dataTrainX,dataTrainY)
    predict=model.predict_proba(dataTestX)
    pro=[x[1] for x in predict]
    t2=time.time()
    print "time:",t2-t1
    return pro,dataTestY
def LRResult(sc,tol=1e-1):
    t1=time.time()
    dataTrainX,dataTrainY = ProcessTrain(sc)
    #dataTestX,dataTestY = ProcessTest(sc)
    dataTestRealX=ProcessRealTest(sc)
    model=LogisticRegression(penalty='l2', tol=tol)
    model.fit(dataTrainX,dataTrainY)
    predict=model.predict_proba(dataTestRealX)
    pro=[x[1] for x in predict]
    t2=time.time()
    print "time:",t2-t1
    return pro
if "__main__"=="__name__":
    LR(sc)