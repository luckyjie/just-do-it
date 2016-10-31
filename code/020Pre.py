# -*-coding:utf_8 -*-
'''
created Date:20161012
Author£:wangjin
data1=sqlContext.sql("select sum(case when Date='null'then 1 else 0 end)as DateNULL,sum(case when Date!='null' then 1 else 0 end)as Date1 from dataTrain")
data2=sqlContext.sql("select count(*) from dataTrain where Date_received='null'")
data11=dataTrain1.map(lambda x:(int(x[0]),int(x[1]),int(x[3]),float(x[4]),int(x[5]),int(ProcesszType(x[6])),int(x[7]),int(ProcessType(x[8]))))
'''
from sklearn.linear_model import LogisticRegression
from sklearn.ensemble import GradientBoostingRegressor, GradientBoostingClassifier
import numpy as np
from pyspark.sql import SQLContext, Row
import datetime, time
import time
from sklearn import metrics


def timeToDatetime(timeNumber, timeEnd):
    timestr = str(timeNumber)
    if (timeEnd == -1):
        return 0
    timestr0 = str(timeEnd)
    timestart = datetime.datetime.strptime(timestr[:4] + '-' + timestr[4:6] + '-' + timestr[6:], '%Y-%m-%d').date()
    timeend = datetime.datetime.strptime(timestr0[:4] + '-' + timestr0[4:6] + '-' + timestr0[6:], '%Y-%m-%d').date()
    timeStart = timestart + datetime.timedelta(days=15)
    if (timeStart >= timeend):
        return 1
    else:
        return 0;


def ProcessType(stringName):
    print stringName
    if stringName == 'null':
        return -1;
    else:
        return int(stringName)


def ProcessFeatures(sc, dataFeatures):  # using Flag to dup useful but failed!
    dataOfflineName = Row('user_id', 'Merchant_id', 'Coupon_id', 'Discount_rate0', 'Discount_rate1', 'Distance','Date_received', 'label')
    dataFea = dataFeatures.map(lambda x: dataOfflineName(*x))
    dataFea1 = sqlC.createDataFrame(dataFea)
    sqlC.registerDataFrameAsTable(dataFea1, "dataFea1")  # user_id,Merchant_id,Coupon_id,
    dataFea2 = sqlC.sql("select Distance,\
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
                                (case when Discount_rate0=-1 and  Discount_rate1=0  then 1 else 0 end)as discount0point0 ,label \
                                from dataFea1")
    dataResultX = dataFea2.map(lambda x: (x[0], x[1], x[2], x[3], x[4], x[5], x[6], x[7], x[8], x[9], x[10], \
                                          x[11], x[12], x[13], x[14], x[15], x[16], x[17], x[18], x[19], \
                                          x[20], x[21], x[22], x[23], x[24], x[25], x[26], x[27], x[28], x[29], \
                                          x[30], x[31], x[32], x[33], x[34], x[35], x[36], x[37], x[38], x[39], \
                                          x[40], x[41], x[42], x[43], x[44], x[45], x[46]))
    # ,x[47],x[48],x[49],x[50],x[51],x[52],x[53],x[54],x[55],x[56],x[57],x[58],x[59]
    dataResultY = dataFea2.map(lambda x: (x[47]))
    return dataResultX, dataResultY


def ProcessFeaturesTest(sc, dataFeatures):  # using Flag to dup useful but failed!
    dataOfflineName = Row('user_id', 'Merchant_id', 'Coupon_id', 'Discount_rate0', 'Discount_rate1', 'Distance','Date_received')
    dataFea = dataFeatures.map(lambda x: dataOfflineName(*x))
    dataFea1 = sqlC.createDataFrame(dataFea)
    sqlC.registerDataFrameAsTable(dataFea1, "dataFea1")  # user_id,Merchant_id,Coupon_id,
    dataFea2 = sqlC.sql("select Distance,\
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
                                (case when Discount_rate0=-1 and  Discount_rate1=0  then 1 else 0 end)as discount0point0  \
                                from dataFea1")
    dataResultTestX = dataFea2.map(lambda x: (x[0], x[1], x[2], x[3], x[4], x[5], x[6], x[7], x[8], x[9], x[10], \
                                              x[11], x[12], x[13], x[14], x[15], x[16], x[17], x[18], x[19], \
                                              x[20], x[21], x[22], x[23], x[24], x[25], x[26], x[27], x[28], x[29], \
                                              x[30], x[31], x[32], x[33], x[34], x[35], x[36], x[37], x[38], x[39], \
                                              x[40], x[41], x[42], x[43], x[44], x[45], x[46]))
    # ,x[47],x[48],x[49]
    # x[50],x[51],x[52],x[53],x[54],x[55],x[56],x[57],x[58],x[59]
    # dataResultY=dataFea2.map(lambda x:(x[50]))
    return dataResultTestX


def ProcessTrain(sc, Date):
    sqlC = SQLContext(sc)
    dataTrain = sqlC.read.parquet("/hadoop/hadoop_/wangjin/ccf_offline_train.parquet")
    sqlC.registerDataFrameAsTable(dataTrain, "dataTrain")
    # modify month
    dataTrain1 = sqlC.sql("select * from dataTrain where Coupon_id!='null' and Date_received>=" + Date + " and Date_received!='null' ")
    sqlC.registerDataFrameAsTable(dataTrain1, "dataTrain1")
    datacount = sqlC.sql("select count(*) from dataTrain1").map(lambda x: int(x[0])).take(1)[0]
    dataTrain2 = dataTrain1.map(lambda x: (int(x[0]), int(x[1]), int(x[2]), float(x[3]), int(x[4]), int(ProcessType(x[5])), int(x[6]), int(ProcessType(x[7]))))
    dataTrain2 = dataTrain2.map(lambda x: (x[0], x[1], x[2], x[3], x[4], x[5], x[6], timeToDatetime(x[6], x[7])))
    # dataOfflineName=Row('user_id','Merchant_id','Coupon_id','Discount_rate0','Discount_rate1','Distance','Date_received','Date','label')
    # dataTrain2=dataTrain2.map(lambda x:dataOfflineName(*x))
    dataResultX, dataResultY = ProcessFeatures(sc, dataTrain2)
    dataTrainX = np.array(dataResultX.take(datacount))
    dataTrainY = np.array(dataResultY.take(datacount))
    # dataTrainX=np.array(dataTrain2.map(lambda x:(x[0],x[1],x[2],x[3],x[4],x[5],x[6])).take(datacount))# Date:x[7]
    # dataTrainY=np.array(dataTrain2.map(lambda x:(x[8])).take(datacount))
    return dataTrainX, dataTrainY

def ProcessRealTest(sc):
    sqlC = SQLContext(sc)
    dataTest = sqlC.read.parquet("/hadoop/hadoop_/wangjin/ccf_offline_test.parquet")
    sqlC.registerDataFrameAsTable(dataTest, "dataTest")
    dataTest1 = sqlC.sql("select * from dataTest where Date_received>='20160701' ")
    sqlC.registerDataFrameAsTable(dataTest1, "dataTest1")
    datacount = sqlC.sql("select count(*) from dataTest1").map(lambda x: int(x[0])).take(1)[0]
    dataTest2 = dataTest1.map(lambda x: (int(x[0]), int(x[1]), int(x[2]), float(x[3]), int(x[4]), int(ProcessType(x[5])), int(x[6])))
    # dataOfflineName=Row('user_id','Merchant_id','Coupon_id','Discount_rate0','Discount_rate1','Distance','Date_received')
    # dataTest2=dataTest2.map(lambda x:dataOfflineName(*x))
    dataResultTestX = ProcessFeaturesTest(sc, dataTest2)
    dataTestRealX = np.array(dataResultTestX.take(datacount))
    return dataTestRealX


def wPretxt(ArrayData, name):
    fr = open("/data/home/hadoop/wangjin/lifeO2O/" + name + '.txt', 'a')
    for i in range(len(ArrayData)):
        fr.write(str(ArrayData[i])  + '\n')
    fr.close()


def LRResult(sc, Date, tol=1e-1):
    t1 = time.time()
    dataTrainX, dataTrainY = ProcessTrain(sc, Date)
    # dataTestX,dataTestY = ProcessTest(sc)
    dataTestRealX = ProcessRealTest(sc)
    model = LogisticRegression(penalty='l2', tol=tol)
    model.fit(dataTrainX, dataTrainY)
    predict = model.predict_proba(dataTestRealX)
    pro = [x[1] for x in predict]
    t2 = time.time()
    print "time:", t2 - t1
    return pro


def GBCresult(sc, Date):
    t1 = time.time()
    dataTrainX, dataTrainY = ProcessTrain(sc, Date)
    # dataTestX,dataTestY = ProcessTest(sc)
    dataTestRealX=ProcessRealTest(sc)
    #dataTestX, dataTestY = ProcessTest(sc)
    # default:loss='deviance','exponential'
    model = GradientBoostingClassifier(loss='deviance', learning_rate=0.1, n_estimators=100, max_depth=3,\
                                       subsample=1, min_samples_split=2, min_samples_leaf=1, \
                                       init=None, random_state=None, max_features=None, verbose=0, max_leaf_nodes=None,\
                                       warm_start=False)
    # model=GradientBoostingRegressor(loss='ls', learning_rate=0.1, n_estimators=100, subsample=1, min_samples_split=2, min_samples_leaf=1,\
    # max_depth=3, init=None, random_state=None, max_features=None, alpha=0.9, verbose=0, max_leaf_nodes=None, warm_start=False)
    model.fit(dataTrainX, dataTrainY)
    # predict=model.predict(dataTestRealX)
    predict = model.predict_proba(dataTestRealX)
    pro = [x[1] for x in predict]
    t2 = time.time()
    print "time:", t2 - t1
    return pro


if "__main__" == "__name__":
    sqlC = SQLContext(sc)
    resultGBCd47_4_6 = GBCresult(sc, '20160401')
    wPretxt(resultGBCd47_4_6, 'd_GBC47_4_6')
    resultGBCd47_5_6 = GBCresult(sc, '20160501')
    wPretxt(resultGBCd47_5_6, 'd_GBC47_5_6')