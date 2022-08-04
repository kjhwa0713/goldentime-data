import pymysql
import requests
import json
import pandas as pd
import xmltodict
import numpy as np
import sqlalchemy as db
pymysql.install_as_MySQLdb()
engine = db.create_engine('mysql+pymysql://hack4:0713@192.168.219.103:3306/hack4')
connection = engine.connect()

url = 'http://apis.data.go.kr/B552657/ErmctInfoInqireService/getEgytBassInfoInqire'
params ={'serviceKey' : 'hfzncuRhJl7xxmV/wnG71VN/zxfeLPlvNfyvgbFTRdow1TGIx9ok+7L7bSx1YmCfKGsCzcOEQFZatI5UU84eyQ==','pageNo' :'1' ,'numOfRows':'100'}

response = requests.get(url, params=params)
jsonStr=json.dumps(xmltodict.parse(response.text))
json_1=json.loads(jsonStr)
total_df=pd.DataFrame(json_1['response']['body']['items']['item'])

for i in range(2,101):
    params['pageNo']=str(i)
    response = requests.get(url, params=params)
    jsonStr=json.dumps(xmltodict.parse(response.text))
    json_1=json.loads(jsonStr)
    df_i=pd.DataFrame(json_1['response']['body']['items']['item'])
    total_df=pd.concat([total_df,df_i],axis=0,ignore_index=True)
df=total_df[total_df['dutyAddr'].str.contains('서울특별시')]
df1=df[df['dutyEryn']=='1']

hospital=df1[['dutyName','dutyAddr','dutyTel3','wgs84Lon','wgs84Lat']]
intermediate=df1[['MKioskTy1','MKioskTy2','MKioskTy3','MKioskTy4','MKioskTy5','MKioskTy6','MKioskTy7','MKioskTy8','MKioskTy10','MKioskTy11']]

hospital=hospital.reset_index(drop=True)
intermediate=intermediate.reset_index(drop=True)

new_sympotom={'hospitalId':0,'deptEng':np.nan}
df=pd.DataFrame([new_sympotom])
intermediate=intermediate.fillna('X')
for i in range(63):
    for j in range(len(intermediate.columns)):
        if intermediate.loc[i][j]=='Y':
            df.loc[df.shape[0]]=[i+1,intermediate.columns[j]]
intermediate=pd.DataFrame(df[1:])
intermediate.to_csv("intermediate.csv", mode='w',index=False)
hospital.to_csv("hospital.csv", mode='w',index=False)


symptom = pd.DataFrame({'deptEng':["MKioskTy1", "MKioskTy2","MKioskTy5","MKioskTy5","MKioskTy11"],
                             'symptomName':["뇌출혈", '뇌졸중',"손가락절단","개에게 물림","소아 머리손상"],
                             'firstAid':[
                           '의식이 없을 때는 목을 뒤로 젖히거나 머리를 옆으로 돌려 기도를 유지하고 목이 조이는 옷이나 넥타이 등을 풀어줘야 한다,구토를 할 때는 바로 눕힌 상태에서 목을 옆으로 돌리고 토물을 손가락을 이용해서 제거해야 한다, 혀가 말리는 것을 방지하기 위해 거즈 같은 것을 말아서 치아에 물려놓는 것도 도움이 된다.',
                             '쇼크상태가 아닌 한 머리를 약간 눕혀서 눕힌 후, 머리 주위, 가슴과 배 등의 옷을 느슨하게 숨쉬기 편하게 해주고, 몸을 담요로 덮어 감싸준다,토사물에 기도가 막히지 않도록 입이나 코로 나오는 침이나 구토물 등을 닦아낸다.',
                             '피가 나는 부위를 심장보다 위에 있도록 한다,깨끗한 천이나 거즈로 감싸고 비닐 봉지에 밀폐하여 얼음이 담긴 통에 보관한다,절단된 부위를 흐르는 깨끗한 물이나 생리식염수로 세척한다',
                             '즉시 흐르는 물로 잘 씻어낸 후 알코올이나 소독약으로 소독한다,피가 지속적으로 흐르는 경우 지그시 압박하여 지혈을 유지한다,광견병 예방 접종력을 알아본다.',
                             '의식이나 환아의 상태를 체크한다,출혈이 심한 상처는 흐르는 물로 닦지 말고, 깨끗한 거즈로 출혈부위를 압박 및 지혈 뒤 병원에 내원한다.'],
                             'notice':[
                            '의식이 혼미한 환자에게 우황청심환 같은 약제를 입에 넣어주면 안된다,뺨을 때리거나 찬물을 뿌리면 안된다,경련이 일어날 경우 목을 꽉 누르면 안된다,피를 통하게 한다고 손끝을 따거나 팔다리를 주무르며 시간을 지체하면 안된다.',
                             '혈압을 높이는 손을 따는 행동등을 하면안된다,폐렴을 유발할 수 있기 때문에 환자에게 먹을 것이나 마실 것을 주지 않는다.',
                             '절단 부위에 지혈제를 뿌리지 않는다,물 또는 얼음에 절단된 신체부위를 직접 담그지 않는다,과도하게 오랫동안 지혈대를 적용하지 않는다',
                             '소주 또는 된장 등의 민간요법을 피한다,개의 이상행동 등을 관찰해야 하므로 개를 죽이거나 다른 곳으로 보내는 행위는 피한다.',
                             '의식이 혼미한 경우 찬물이나 기응환 등을 먹이지 않는다,꼭 이동해야 하는 상황이 아니라면 119에 신고하고 아이를 그대로 눕혀둔다.']
                             })

department=pd.DataFrame({'deptEng':["MKioskTy1", "MKioskTy2", "MKioskTy3","MKioskTy4","MKioskTy5","MKioskTy6","MKioskTy7","MKioskTy8","MKioskTy10","MKioskTy11"],
                             'deptKor':["뇌출혈수술", '뇌경색의재관류', '심근경색의재관류',"복부손상의수술","사지접합의수술","응급내시경","응급투석","조산산모","정신질환자","신생아"]})


hospital.index=hospital.index + 1

hospital.to_sql(name='hospital', con=connection, if_exists='replace', index=True)
intermediate.to_sql(name='intermediate', con=connection, if_exists='replace', index=True)
symptom.to_sql(name='symptom', con=connection, if_exists='replace', index=True)
department.to_sql(name='department', con=connection, if_exists='replace', index=True)
