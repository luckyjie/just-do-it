#-*-coding:utf_8 -*-
'''
created Date:20161012
Author£:wangjin
data1=sqlContext.sql("select sum(case when Date='null'then 1 else 0 end)as DateNULL,sum(case when Date!='null' then 1 else 0 end)as Date1 from dataTrain")
data2=sqlContext.sql("select count(*) from dataTrain where Date_received='null'")
data11=dataTrain1.map(lambda x:(int(x[0]),int(x[1]),int(x[3]),float(x[4]),int(x[5]),int(ProcesszType(x[6])),int(x[7]),int(ProcessType(x[8]))))

'''
import xgboost as xgb
from sklearn.linear_model import LogisticRegression
from sklearn.ensemble import GradientBoostingRegressor,GradientBoostingClassifier
import numpy as np
from pyspark.sql import SQLContext,Row
import datetime,time
import time
from sklearn import metrics
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
def ProcessGroupFeatures(sc,dataFea1):
    '''
    You can try  online_train data  to extract  groupby features
    based on  user_id
    dataOnlineTrain1=dataonlineTrain.groupby('user_id')
    of course: offline_train data  ,coupon_ip ,discount_rate0 to  groupby features
    dataFeaTemp4 = dataFea1.groupby(['Coupon_id', 'Discount_rate0']).count().toDF('Coupon_id', 'Discount_rate0', 'Cou_Disc_count')
    '''
    dataFeaTemp = dataFea1.groupby(['user_id', 'Merchant_id']).count().toDF('user_id','Merchant_id','user_Mer_count')
    dataFeaTemp1 = dataFea1.groupby(['Merchant_id']).count().toDF('Merchant_id','Mer_count')
    dataFeaTemp2 = dataFea1.groupby(['Merchant_id','Coupon_id']).count().toDF('Merchant_id', 'Coupon_id','Mer_Cou_count')
    dataFeaTemp3 = dataFea1.groupby(['Merchant_id', 'Coupon_id','Distance']).count().toDF('Merchant_id', 'Coupon_id','Distance' ,'Mer_Cou_Dis_count')
    dataFeaTemp4 = dataFea1.groupby(['Merchant_id', 'Distance']).count().toDF('Merchant_id', 'Distance', 'Mer_Dis_count')
    #dataFeaTemp5 = dataFea1.groupby(['user_id', 'Distance']).count().toDF('user_id', 'Distance','user_Dis_count')
    #merge groupby count as Features
    dataFeaTempSum=dataFeaTemp.join(dataFeaTemp1,'Merchant_id').join(dataFeaTemp2,'Merchant_id').join(dataFeaTemp4,'Merchant_id').join(dataFeaTemp3,['Merchant_id','Coupon_id','Distance'])
    sqlC.registerDataFrameAsTable(dataFeaTempSum, "dataFeaTempSum")
    #  max   user_Mer_count  Mer_count  Mer_Cou_count  Mer_Cou_Dis_count  Mer_Dis_count
    dataFeaMax=sqlC.sql("select max(user_Mer_count) as Max_user_Mer_count,max(Mer_count) as Max_Mer_count,\
                                max(Mer_Cou_count) as Max_Mer_Cou_count, max(Mer_Cou_Dis_count) as Max_Mer_Cou_Dis_count,\
                                max (Mer_Dis_count) as Max_Mer_Dis_count from dataFeaTempSum")
    sqlC.registerDataFrameAsTable(dataFeaMax, "dataFeaMax")
    #merge table   based on  'user_id', 'Merchant_id','Coupon_id','Distance'
    dataFeaSum=dataFea1.join(dataFeaTempSum,['user_id', 'Merchant_id','Coupon_id','Distance'])
    sqlC.registerDataFrameAsTable(dataFeaSum, "dataFeaSum")
    #A.user_id,A.Merchant_id,A.Coupon_id,A.Distance,A.Discount_rate0,A.Discount_rate1,A.Date_received,
    dataFeaResult = sqlC.sql("select A.*,\
                            (A.user_Mer_count/B.Max_user_Mer_count) as user_Mer_count,\
                            (A.Mer_count/B.Max_Mer_count) as Mer_count,\
                            (A.Mer_Cou_count/B.Max_Mer_Cou_count) as Mer_Cou_count,\
                         (A.Mer_Cou_Dis_count/B.Max_Mer_Cou_Dis_count) as Mer_Cou_Dis_count,\
                         (A.Mer_Dis_count/B.Max_Mer_Dis_count) as Mer_Dis_count ,A.label\
                         from dataFeaSum A,dataFeaMax B")
    return dataFeaResult
def ProcessFeatures(sc,dataFeatures):#using Flag to dup useful but failed!
    dataOfflineName=Row('user_id','Merchant_id','Coupon_id','Discount_rate0','Discount_rate1','Distance','Date_received','label')
    dataFea=dataFeatures.map(lambda x:dataOfflineName(*x))
    dataFea1=sqlC.createDataFrame(dataFea)
    dataFea1=ProcessGroupFeatures(sc,dataFea1)
    sqlC.registerDataFrameAsTable(dataFea1,"dataFea1")#user_id,Merchant_id,Coupon_id,Distance,
    dataFea2=sqlC.sql("select \
                                (case when Distance=-1 then 1 else 0 end)as distance_1,\
                                (case when Distance=1  then 1 else 0 end)as distance1,\
                                (case when Distance=2  then 1 else 0 end)as distance2,\
                                (case when Distance=3  then 1 else 0 end)as distance3,\
                                (case when Distance=4  then 1 else 0 end)as distance4,\
                                (case when Distance=5  then 1 else 0 end)as distance5,\
                                (case when Distance=6  then 1 else 0 end)as distance6,\
                                (case when Distance=7  then 1 else 0 end)as distance7,\
                                (case when Distance=8  then 1 else 0 end)as distance8,\
                                (case when Distance=9  then 1 else 0 end)as distance9,\
                                (case when Distance=10 then 1 else 0 end)as distance10,\
                                (case when Discount_rate0=0.2  then 1 else 0 end)as discount0point2,\
                                (case when Discount_rate0=0.5  then 1 else 0 end)as discount0point5,\
                                (case when Discount_rate0=0.6  then 1 else 0 end)as discount0point6,\
                                (case when Discount_rate0=0.7  then 1 else 0 end)as discount0point7,\
                                (case when Discount_rate0=0.75 then 1 else 0 end)as discount0point75,\
                                (case when Discount_rate0=0.8  then 1 else 0 end)as discount0point8,\
                                (case when Discount_rate0=0.85 then 1 else 0 end)as discount0point85,\
                                (case when Discount_rate0=0.9  then 1 else 0 end)as discount0point9,\
                                (case when Discount_rate0=0.95 then 1 else 0 end)as discount0point95,\
                                (case when Discount_rate0=5  and  Discount_rate1=1   then 1 else 0 end)as discount5point1,\
                                (case when Discount_rate0=10 and  Discount_rate1=1   then 1 else 0 end)as discount10point1,\
                                (case when Discount_rate0=10 and  Discount_rate1=5   then 1 else 0 end)as discount10point5,\
                                (case when Discount_rate0=20 and  Discount_rate1=1   then 1 else 0 end)as discount20point1,\
                                (case when Discount_rate0=20 and  Discount_rate1=5   then 1 else 0 end)as discount20point5,\
                                (case when Discount_rate0=20 and  Discount_rate1=10  then 1 else 0 end)as discount20point10,\
                                (case when Discount_rate0=30 and  Discount_rate1=1   then 1 else 0 end)as discount30point1,\
                                (case when Discount_rate0=30 and  Discount_rate1=5   then 1 else 0 end)as discount30point5,\
                                (case when Discount_rate0=30 and  Discount_rate1=10  then 1 else 0 end)as discount30point10,\
                                (case when Discount_rate0=30 and  Discount_rate1=20  then 1 else 0 end)as discount30point20,\
                                (case when Discount_rate0=50 and  Discount_rate1=1   then 1 else 0 end)as discount50point1,\
                                (case when Discount_rate0=50 and  Discount_rate1=5   then 1 else 0 end)as discount50point5,\
                                (case when Discount_rate0=50 and  Discount_rate1=10  then 1 else 0 end)as discount50point10,\
                                (case when Discount_rate0=50 and  Discount_rate1=20  then 1 else 0 end)as discount50point20,\
                                (case when Discount_rate0=50 and  Discount_rate1=30   then 1 else 0 end)as discount50point30,\
                                (case when Discount_rate0=100 and  Discount_rate1=1   then 1 else 0 end)as discount100point1,\
                                (case when Discount_rate0=100 and  Discount_rate1=5   then 1 else 0 end)as discount100point5,\
                                (case when Discount_rate0=100 and  Discount_rate1=10  then 1 else 0 end)as discount100point10,\
                                (case when Discount_rate0=100 and  Discount_rate1=20  then 1 else 0 end)as discount100point20,\
                                (case when Discount_rate0=100 and  Discount_rate1=30  then 1 else 0 end)as discount100point30,\
                                (case when Discount_rate0=100 and  Discount_rate1=50  then 1 else 0 end)as discount100point50,\
                                (case when Discount_rate0=150 and  Discount_rate1=5   then 1 else 0 end)as discount100point1,\
                                (case when Discount_rate0=150 and  Discount_rate1=10  then 1 else 0 end)as discount150point10,\
                                (case when Discount_rate0=150 and  Discount_rate1=20  then 1 else 0 end)as discount150point20,\
                                (case when Discount_rate0=150 and  Discount_rate1=30  then 1 else 0 end)as discount150point30,\
                                (case when Discount_rate0=150 and  Discount_rate1=50  then 1 else 0 end)as discount150point50,\
                                (case when Discount_rate0=200 and  Discount_rate1=5  then 1 else 0 end)as discount200point5,\
                                (case when Discount_rate0=200 and  Discount_rate1=10  then 1 else 0 end)as discount200point10,\
                                (case when Discount_rate0=200 and  Discount_rate1=20  then 1 else 0 end)as discount200point20,\
                                (case when Discount_rate0=200 and  Discount_rate1=30  then 1 else 0 end)as discount200point30,\
                                (case when Discount_rate0=200 and  Discount_rate1=50  then 1 else 0 end)as discount200point50,\
                                (case when Discount_rate0=200 and  Discount_rate1=100  then 1 else 0 end)as discount200point100,\
                                (case when Discount_rate0=300 and  Discount_rate1=10  then 1 else 0 end)as discount300point10,\
                                (case when Discount_rate0=300 and  Discount_rate1=20  then 1 else 0 end)as discount300point20,\
                                (case when Discount_rate0=300 and  Discount_rate1=30  then 1 else 0 end)as discount300point30,\
                                (case when Discount_rate0=300 and  Discount_rate1=50  then 1 else 0 end)as discount300point50,\
                                (case when Discount_rate0=-1 and  Discount_rate1=0  then 1 else 0 end)as discount0point0 ,\
                                user_Mer_count ,Mer_count , Mer_Cou_count,  Mer_Cou_Dis_count,  Mer_Dis_count,label \
                                from dataFea1")
    dataResultX=dataFea2.map(lambda x:(x[0],x[1],x[2],x[3],x[4],x[5],x[6],x[7],x[8],x[9],x[10],\
                                      x[11],x[12],x[13],x[14],x[15],x[16],x[17],x[18],x[19],\
                                      x[20],x[21],x[22],x[23],x[24],x[25],x[26],x[27],x[28],x[29],\
                                      x[30],x[31],x[32],x[33],x[34],x[35],x[36],x[37],x[38],x[39],\
                                      x[40],x[41],x[42],x[43],x[44],x[45],x[46],x[47],x[48],x[49],
                                      x[50],x[51],x[52],x[53],x[54],x[55],x[56],x[57],x[58],x[59],
                                      x[60], x[61]))
    #
    dataResultY=dataFea2.map(lambda x:(x[62]))
    return dataResultX,dataResultY
def ProcessFeaturesTest(sc,dataFeatures):#using Flag to dup useful but failed!
    dataOfflineName=Row('user_id','Merchant_id','Coupon_id','Discount_rate0','Discount_rate1','Distance','Date_received')
    dataFea=dataFeatures.map(lambda x:dataOfflineName(*x))
    dataFea1=sqlC.createDataFrame(dataFea)
    dataFea1 = ProcessGroupFeatures(sc, dataFea1)
    sqlC.registerDataFrameAsTable(dataFea1,"dataFea1")#user_id,Merchant_id,Coupon_id,Distance,
    dataFea2=sqlC.sql("select \
                                    (case when Distance=-1 then 1 else 0 end)as distance_1,\
                                    (case when Distance=1  then 1 else 0 end)as distance1,\
                                    (case when Distance=2  then 1 else 0 end)as distance2,\
                                    (case when Distance=3  then 1 else 0 end)as distance3,\
                                    (case when Distance=4  then 1 else 0 end)as distance4,\
                                    (case when Distance=5  then 1 else 0 end)as distance5,\
                                    (case when Distance=6  then 1 else 0 end)as distance6,\
                                    (case when Distance=7  then 1 else 0 end)as distance7,\
                                    (case when Distance=8  then 1 else 0 end)as distance8,\
                                    (case when Distance=9  then 1 else 0 end)as distance9,\
                                    (case when Distance=10 then 1 else 0 end)as distance10,\
                                (case when Discount_rate0=0.2  then 1 else 0 end)as discount0point2,\
                                (case when Discount_rate0=0.5  then 1 else 0 end)as discount0point5,\
                                (case when Discount_rate0=0.6  then 1 else 0 end)as discount0point6,\
                                (case when Discount_rate0=0.7  then 1 else 0 end)as discount0point7,\
                                (case when Discount_rate0=0.75 then 1 else 0 end)as discount0point75,\
                                (case when Discount_rate0=0.8  then 1 else 0 end)as discount0point8,\
                                (case when Discount_rate0=0.85 then 1 else 0 end)as discount0point85,\
                                (case when Discount_rate0=0.9  then 1 else 0 end)as discount0point9,\
                                (case when Discount_rate0=0.95 then 1 else 0 end)as discount0point95,\
                                (case when Discount_rate0=5  and  Discount_rate1=1   then 1 else 0 end)as discount5point1,\
                                (case when Discount_rate0=10 and  Discount_rate1=1   then 1 else 0 end)as discount10point1,\
                                (case when Discount_rate0=10 and  Discount_rate1=5   then 1 else 0 end)as discount10point5,\
                                (case when Discount_rate0=20 and  Discount_rate1=1   then 1 else 0 end)as discount20point1,\
                                (case when Discount_rate0=20 and  Discount_rate1=5   then 1 else 0 end)as discount20point5,\
                                (case when Discount_rate0=20 and  Discount_rate1=10  then 1 else 0 end)as discount20point10,\
                                (case when Discount_rate0=30 and  Discount_rate1=1   then 1 else 0 end)as discount30point1,\
                                (case when Discount_rate0=30 and  Discount_rate1=5   then 1 else 0 end)as discount30point5,\
                                (case when Discount_rate0=30 and  Discount_rate1=10  then 1 else 0 end)as discount30point10,\
                                (case when Discount_rate0=30 and  Discount_rate1=20  then 1 else 0 end)as discount30point20,\
                                (case when Discount_rate0=50 and  Discount_rate1=1   then 1 else 0 end)as discount50point1,\
                                (case when Discount_rate0=50 and  Discount_rate1=5   then 1 else 0 end)as discount50point5,\
                                (case when Discount_rate0=50 and  Discount_rate1=10  then 1 else 0 end)as discount50point10,\
                                (case when Discount_rate0=50 and  Discount_rate1=20  then 1 else 0 end)as discount50point20,\
                                (case when Discount_rate0=50 and  Discount_rate1=30   then 1 else 0 end)as discount50point30,\
                                (case when Discount_rate0=100 and  Discount_rate1=1   then 1 else 0 end)as discount100point1,\
                                (case when Discount_rate0=100 and  Discount_rate1=5   then 1 else 0 end)as discount100point5,\
                                (case when Discount_rate0=100 and  Discount_rate1=10  then 1 else 0 end)as discount100point10,\
                                (case when Discount_rate0=100 and  Discount_rate1=20  then 1 else 0 end)as discount100point20,\
                                (case when Discount_rate0=100 and  Discount_rate1=30  then 1 else 0 end)as discount100point30,\
                                (case when Discount_rate0=100 and  Discount_rate1=50  then 1 else 0 end)as discount100point50,\
                                (case when Discount_rate0=150 and  Discount_rate1=5   then 1 else 0 end)as discount100point1,\
                                (case when Discount_rate0=150 and  Discount_rate1=10  then 1 else 0 end)as discount150point10,\
                                (case when Discount_rate0=150 and  Discount_rate1=20  then 1 else 0 end)as discount150point20,\
                                (case when Discount_rate0=150 and  Discount_rate1=30  then 1 else 0 end)as discount150point30,\
                                (case when Discount_rate0=150 and  Discount_rate1=50  then 1 else 0 end)as discount150point50,\
                                (case when Discount_rate0=200 and  Discount_rate1=5  then 1 else 0 end)as discount200point5,\
                                (case when Discount_rate0=200 and  Discount_rate1=10  then 1 else 0 end)as discount200point10,\
                                (case when Discount_rate0=200 and  Discount_rate1=20  then 1 else 0 end)as discount200point20,\
                                (case when Discount_rate0=200 and  Discount_rate1=30  then 1 else 0 end)as discount200point30,\
                                (case when Discount_rate0=200 and  Discount_rate1=50  then 1 else 0 end)as discount200point50,\
                                (case when Discount_rate0=200 and  Discount_rate1=100  then 1 else 0 end)as discount200point100,\
                                (case when Discount_rate0=300 and  Discount_rate1=10  then 1 else 0 end)as discount300point10,\
                                (case when Discount_rate0=300 and  Discount_rate1=20  then 1 else 0 end)as discount300point20,\
                                (case when Discount_rate0=300 and  Discount_rate1=30  then 1 else 0 end)as discount300point30,\
                                (case when Discount_rate0=300 and  Discount_rate1=50  then 1 else 0 end)as discount300point50,\
                                (case when Discount_rate0=-1 and  Discount_rate1=0  then 1 else 0 end)as discount0point0 ,\
                                user_Mer_count ,Mer_count , Mer_Cou_count,  Mer_Cou_Dis_count,  Mer_Dis_count  \
                                from dataFea1")
    dataResultTestX=dataFea2.map(lambda x:(x[0],x[1],x[2],x[3],x[4],x[5],x[6],x[7],x[8],x[9],x[10],\
                                      x[11],x[12],x[13],x[14],x[15],x[16],x[17],x[18],x[19],\
                                      x[20],x[21],x[22],x[23],x[24],x[25],x[26],x[27],x[28],x[29],\
                                      x[30],x[31],x[32],x[33],x[34],x[35],x[36],x[37],x[38],x[39],\
                                      x[40],x[41],x[42],x[43],x[44],x[45],x[46],x[47],x[48],x[49],
                                      x[50], x[51], x[52], x[53], x[54], x[55], x[56], x[57], x[58], x[59],
                                      x[60], x[61]))
    #
    #x[60],x[61],x[62],x[63],x[64],x[65],x[66]
    return dataResultTestX
def  ProcessTrain(sc,Date,path):
    sqlC=SQLContext(sc)
    dataTrain=sqlC.read.parquet(path)#'/hadoop/hadoop_/wangjin/ccf_offline_train.parquet'
    sqlC.registerDataFrameAsTable(dataTrain,"dataTrain")
    #modify month
    dataTrain1=sqlC.sql("select * from dataTrain where Coupon_id!='null' and Date_received>="+Date+" and Date_received<'20160701' and Date_received!='null' ")
    sqlC.registerDataFrameAsTable(dataTrain1,"dataTrain1")
    datacount=sqlC.sql("select count(*) from dataTrain1").map(lambda x:int(x[0])).take(1)[0]
    dataTrain2=dataTrain1.map(lambda x:(int(x[0]),int(x[1]),int(x[2]),float(x[3]),int(x[4]),int(ProcessType(x[5])),int(x[6]),int(ProcessType(x[7]))))
    dataTrain2=dataTrain2.map(lambda x:(x[0],x[1],x[2],x[3],x[4],x[5],x[6],timeToDatetime(x[6],x[7])))
    #dataOfflineName=Row('user_id','Merchant_id','Coupon_id','Discount_rate0','Discount_rate1','Distance','Date_received','Date','label')
    #dataTrain2=dataTrain2.map(lambda x:dataOfflineName(*x))
    dataResultX,dataResultY=ProcessFeatures(sc,dataTrain2)
    dataTrainX=np.array(dataResultX.take(datacount))
    dataTrainY=np.array(dataResultY.take(datacount))
    #dataTrainX=np.array(dataTrain2.map(lambda x:(x[0],x[1],x[2],x[3],x[4],x[5],x[6])).take(datacount))# Date:x[7]
    #dataTrainY=np.array(dataTrain2.map(lambda x:(x[8])).take(datacount))
    return dataTrainX,dataTrainY
def ProcessTest(sc,path):
    sqlC=SQLContext(sc)
    dataTest=sqlC.read.parquet(path)
    sqlC.registerDataFrameAsTable(dataTest,"dataTest")
    dataTest1=sqlC.sql("select * from dataTest where Date_received>='20160601' and Date_received!='null' ")
    sqlC.registerDataFrameAsTable(dataTest1,"dataTest1")
    #print "123..."
    datacount=sqlC.sql("select count(*) from dataTest1").map(lambda x:int(x[0])).take(1)[0]
    dataTest2=dataTest1.map(lambda x:(int(x[0]),int(x[1]),int(x[2]),float(x[3]),int(x[4]),int(ProcessType(x[5])),int(x[6]),int(ProcessType(x[7]))))
    dataTest2=dataTest2.map(lambda x:(x[0],x[1],x[2],x[3],x[4],x[5],x[6],timeToDatetime(x[6],x[7])))
    #dataOfflineName=Row('user_id','Merchant_id','Coupon_id','Discount_rate0','Discount_rate1','Distance','Date_received','Date','label')
    #dataTest2=dataTest2.map(lambda x:dataOfflineName(*x))
    dataTestX=np.array(dataTest2.map(lambda x:(x[0],x[1],x[2],x[3],x[4],x[5],x[6])).take(datacount)) #Date :x[7]
    dataTestY=np.array(dataTest2.map(lambda x:(x[7])).take(datacount))
    dataTestX = ProcessFeaturesTest(sc, dataTest2)
    dataTestX = np.array(dataTestX.take(datacount))
    return dataTestX,dataTestY
def wPretxt(ArrayData,name):
    fr=open('/data/home/hadoop/wangjin/'+name+'.txt','a')
    #fr1=open('/data/home/hadoop/wangjin/test.txt','a')
    for i in range(len(ArrayData)):
        fr.write(str(ArrayData[i])+'\n')
    fr.close()
def LRTest(sc,Date,tol=1e-1):
    t1=time.time()
    dataTrainX,dataTrainY = ProcessTrain(sc,Date,path)
    dataTestX,dataTestY = ProcessTest(sc,path)
    #dataTestRealX=ProcessRealTest(sc)
    model=LogisticRegression(penalty='l2', tol=tol)
    model.fit(dataTrainX,dataTrainY)
    predict=model.predict_proba(dataTestX)
    pro=[x[1] for x in predict]
    t2=time.time()
    print "time:",t2-t1
    return pro,dataTestY
def GBCresult(sc,Date,path):
    t1=time.time()
    dataTrainX,dataTrainY = ProcessTrain(sc,Date,path)
    dataTestX,dataTestY = ProcessTest(sc,path)
    #dataTestX,dataTestY=ProcessRealTest(sc)
    #default:loss='exponential','deviance'
    model=GradientBoostingClassifier(loss='deviance', learning_rate=0.1, n_estimators=100, max_depth=3,subsample=0.8, min_samples_split=2, min_samples_leaf=1,\
     init=None, random_state=None, max_features=None,  verbose=0, max_leaf_nodes=None, warm_start=False)
    #model=GradientBoostingRegressor(loss='ls', learning_rate=0.1, n_estimators=100, subsample=1, min_samples_split=2, min_samples_leaf=1,\
    #max_depth=3, init=None, random_state=None, max_features=None, alpha=0.9, verbose=0, max_leaf_nodes=None, warm_start=False)
    model.fit(dataTrainX,dataTrainY)
    #predict=model.predict(dataTestRealX)
    predict=model.predict_proba(dataTestX)
    pro=[x[1] for x in predict]
    t2=time.time()
    print "time:",t2-t1
    return pro,dataTestYdef XGBoost(sc,Date,path):
    t1 = time.time()
    dataTrainX, dataTrainY = ProcessTrain(sc, Date, path)
    dataTestX, dataTestY = ProcessTest(sc, path)
    # default:loss='exponential','deviance'
    model = xgb.XGBClassifier()
    model.fit(dataTrainX, dataTrainY)
    # predict=model.predict(dataTestRealX)
    predict = model.predict_proba(dataTestX)
    pro = [x[1] for x in predict]
    t2 = time.time()
    print "time:", t2 - t1
    return pro, dataTestY
def XGBC(sc,path):
    # wangjin    path='/data/home/hadoop/wangjin/lifeO2O/'
    sqlC = SQLContext(sc)
    fr = open('/data/home/hadoop/wangjin/lifeO2O/resultAUC.txt', 'a')
    fr.write('57XGBC:'+'\n')
    for i in [ '20160401', '20160501']:#'20160101','20160201','20160301'
        print 'i:', i
        resultGBC47_1_5, dataTestY = XGBoost(sc, i,path)
        y_scores = np.array(resultGBC47_1_5)
        test_auc = metrics.roc_auc_score(dataTestY, y_scores)
        fr.write(str(i) + ":" + str(test_auc) + '\n')
        print "AUC:", test_auc
    fr.close()
def GBC(sc,path):
    # wangjin    path='/hadoop/hadoop_/wangjin/ccf_offline_train.parquet'
    sqlC = SQLContext(sc)
    fr = open('/data/home/hadoop/wangjin/lifeO2O/resultAUC.txt', 'a')
    fr.write('dGBC:'+'\n')
    for i in [ '20160401', '20160501']:#'20160101','20160201','20160301'
        print 'i:', i
        resultGBC47_1_5, dataTestY = GBCresult(sc, i,path)
        y_scores = np.array(resultGBC47_1_5)
        test_auc = metrics.roc_auc_score(dataTestY, y_scores)
        fr.write(str(i) + ":" + str(test_auc) + '\n')
        print "AUC:", test_auc
    fr.close()

def LR(sc,path):
    #wangjin    path='/hadoop/hadoop_/wangjin/ccf_offline_train.parquet'
    sqlC=SQLContext(sc)
    #modify   save file path
    fr=open('/data/home/hadoop/wangjin/lifeO2O/resultAUC.txt','a')
    fr.write('LR:' + '\n')
    for i in ['20160401','20160501']:#'20160101','20160201','20160301',
        print 'i:',i
        resultLR47_1_5,dataTestY=LRTest(sc,i,path)
        y_scores = np.array(resultLR47_1_5)
        test_auc = metrics.roc_auc_score(dataTestY, y_scores)
        fr.write(str(i)+":"+str(test_auc)+'\n')
        print "AUC:",test_auc
    fr.close()
if "__name"=="__main__":
    sqlC = SQLContext(sc)
    GBC(sc,'/hadoop/hadoop_/wangjin/ccf_offline_train.parquet')
    LR(sc,'/hadoop/hadoop_/wangjin/ccf_offline_train.parquet')
