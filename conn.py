import pymysql
import requests
import json
import pandas as pd
import xmltodict
import numpy as np
import sqlalchemy as db
pymysql.install_as_MySQLdb()
engine = db.create_engine('mysql+pymysql://hack4:0713@192.168.219.106:3306/hack4')
connection = engine.connect()

# 데이터 받아옴
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
intermediate=df1[['MKioskTy1','MKioskTy2','MKioskTy3','MKioskTy4','MKioskTy5','MKioskTy6','MKioskTy7','MKioskTy8','MKioskTy9','MKioskTy10','MKioskTy11']]

hospital=hospital.reset_index(drop=True)
intermediate=intermediate.reset_index(drop=True)

# intermediate table 정의
new_sympotom={'HospitalId':0,'deptEng':np.nan}
df=pd.DataFrame([new_sympotom])
intermediate=intermediate.fillna('X')
for i in range(63):
    for j in range(len(intermediate.columns)):
        if intermediate.loc[i][j]=='Y':
            df.loc[df.shape[0]]=[i+1,intermediate.columns[j]]
intermediate=pd.DataFrame(df[1:])

# department table 정의
department=pd.DataFrame({'deptEng':["MKioskTy1", "MKioskTy2", "MKioskTy3","MKioskTy4","MKioskTy5","MKioskTy6","MKioskTy7","MKioskTy8","MKioskTy9","MKioskTy10","MKioskTy11"],
                             'deptKor':["뇌출혈수술", '뇌경색의재관류', '심근경색의재관류',"복부손상의수술","사지접합의수술","응급내시경","응급투석","조산산모","정신질환자","신생아","중증화상"]})

# department table 과 intermediate table 1 : n
deptId=[]
for i in intermediate.deptEng:
    deptId.append(department[department['deptEng']==i].index[0]+1)
intermediate['DepartmentId']=deptId

intermediate.to_csv("intermediate.csv", mode='w',index=False)
hospital.to_csv("hospital.csv", mode='w',index=False)

# symptom table 정의
symptom = pd.DataFrame({'deptEng':[ "MKioskTy1","MKioskTy1", "MKioskTy2","MKioskTy3","MKioskTy4","MKioskTy5","MKioskTy5","MKioskTy5","MKioskTy6","MKioskTy7","MKioskTy7","MKioskTy8","MKioskTy9","MKioskTy10","MKioskTy10","MKioskTy10","MKioskTy11"],
                        'symptomName':["의식장애", "수족마비",'뇌경색','심근경색',"복부창상","손가락절단","개에게물림","신체절단","위경련","혈액투석","신장투석","조산","과호흡","소아머리손상","소아열성경련","소아구토","화상"],
                        'firstAid':[
                            #1 의식장애 MKioskTy1 ㅇ
                           '호흡이 멈추었는가 어떤가를 알기 위해서는 환자 가슴의 움직임을 바로 살펴 봐야 한다.만약 호흡이 없다면 곧바로 인공호흡을 실시한다.옆으로 눕혀 몸이 흔들리지 않도록 하면서 위로 향한 쪽의 무릎을 약간 앞으로 굽혀 준다.',
                            #2 수족마비 MKioskTy1 ㅇ
                            '마비된 쪽을 위로 하여 옆으로 눕힌다.쇼크상태가 아니라면 머리는 약간 높여주고 옷은 느슨하게 해준다.마비증상을 호소하면서 의식을 잃고 쓰러졌을 경우, 환자를 흔들지 말고 머리와 목을 지지해준 후 겨드랑이에 손을 넣어 평평한 곳으로 가만히 옮긴다.',
                            #3 뇌경색 MKioskTy2 ㅇ
                            '쇼크상태가 아닌 한 머리를 약간 눕혀서 눕힌 후, 머리 주위, 가슴과 배 등의 옷을 느슨하게 숨쉬기 편하게 해주고, 몸을 담요로 덮어 감싸준다.토사물에 기도가 막히지 않도록 입이나 코로 나오는 침이나 구토물 등을 닦아낸다.',
                            #4 심근경색 MKioskTy3 ㅇ
                            '움직이지 않고 평편한 곳에 눕힌다. 만약 심정지가 된 상태라면 심폐소생술을 통해 심장 근육의 괴사를 막는다.',
                            #5 복부창상 MKioskTy4 ㅇ
                            '구토에 대비하여 고개를 한쪽으로 하고 기도에 이물질이 들어가지 않도록 주의한다.다리를 구부리고 무릎 밑을 담요나 베개 등으로 받쳐 주어 통증을 경감시키고 복부 근육의 긴장을 풀어 준다.원활한 산소공급을 돕고 쇼크상태에 빠지지 않도록 주의한다.약 노출된 내장, 장기 등이 있다면 절대로 건드리면 안 되며 체온을 잃지 않도록 타올 등을 얹어 준다.',
                            #6 손가락절단 MKioskTy5 ㅇ
                             '피가 나는 부위를 심장보다 위에 있도록 한다.깨끗한 천이나 거즈로 감싸고 비닐 봉지에 밀폐하여 얼음이 담긴 통에 보관한다.절단된 부위를 흐르는 깨끗한 물이나 생리식염수로 세척한다',
                            #7 개에게물림 MKioskTy5 ㅇ
                              '즉시 흐르는 물로 잘 씻어낸 후 알코올이나 소독약으로 소독한다.피가 지속적으로 흐르는 경우 지그시 압박하여 지혈을 유지한다.광견병 예방 접종력을 알아본다.',
                            #8 신체절단 MKioskTy5 ㅇ
                             '떨어진 신체 부분을 생리식염수로 가볍게 세척하고 난 후, 생리식염수로 적신 소독된 거즈로 감싼다. 거즈로 감싼 신체 부분을 뚫어지지 않은 비닐봉지에 담거나 랩으로 단단히 밀봉한다. 신체 부분을 담은 비닐봉지를 이 얼음을 띄운 용기에 넣어서 보관하면 된다.',
                            #9 위경련 MKioskTy6 세빈
                            '명치 끝을 지압하고 명치 부분을 온찜질한다.등을 움크리는 자세를 취한다.따뜻한 물을 마신다.',
                            #10 혈액투석 MKioskTy7 ㅇ
                            '응급실로 내원하세요',
                            #11 신장투석 MKioskTy7  ㅇ
                            '응급실로 내원하세요',
                            #12 조산 MKioskTy8
                            '응급실로 내원하세요',
                            #13 과호흡 MKioskTy9 ㅇ
                             '넥타이, 벨트, 타이즈 등 몸을 조이는 것을 느슨하게 풀어주어 몸을 편하게 한다.숨을 참고 천천히 쉬도록 한다.종이 봉지를 뒤집어 입에 씌우고 호흡을 유도하며 안정제를 투여한다.',
                            #14 소아머리손상 MKioskTy10 ㅇ
                             '의식이나 환아의 상태를 체크한다.출혈이 심한 상처는 흐르는 물로 닦지 말고, 깨끗한 거즈로 출혈부위를 압박 및 지혈 뒤 병원에 내원한다.',
                            #15 소아열성경련MKioskTy10 ㅇ
                            '옷을 벗긴 다음 미지근한 물을 적신 수건으로 가슴, 등, 머리 목, 팔다리를 닦아주고 그래도 열이 내리지 않으면 차차 차가운 물로 전신을 닦아준다.아이를 옆으로 비스듬히 눕히고 머리를 약간 아래쪽으로 두어 입안의 타액이 밖으로 흘러나오게 하고 호흡을 더욱자유롭게 해 주어야 한다.',
                            #16 소아구토 MKioskTy10 ㅇ
                            '아이를 앉히거나 눕혀서 구토물이 기도로 들어가지 않도록 한다.아무것도 먹이지 말고 탈수가 되지 않도록 수액 주사를 한다.신생아의 경우 먹은 후 꼭 트림을 시켜준다',
                            #17 화상 MKioskTy11 ㅇ
                            '냉찜질을 위해 흐르는 수돗물이나 얼음물고 충분히(적어도 15~30분 정도) 식힌다.옷을 입은 채 뜨거운 물을 뒤집어 쓴 경우에는, 우선 찬물을 끼얹어 식힌 후 옷을 벗겨야 한다.옷이 상처 주위에 달라붙은 경우에는 피부에 손상을 줄 수 있으므로 무리하게 옷을 떼내지 않고, 가위로 옷을 잘라주는 방법을 쓰며 찢어서 벗길 때도 피부가 상하지 않도록 주의해야 한다.',
                             ],
                             
                             'notice':[
                            #1 의식장애
                            '의식을 잃으면 토한 것을 토해낼 수 없어 목이 막혀 질식을 일으키기도 하고, 폐로 들어가 폐렴을 일으킬 위험이 있으므로 머리를 높이지 않는다. 어린이의 경우 기도 확보 시 어린이 목은 유연해서 얼굴을 위로 보게 한 채 머리를 강하 게 젖히면 오히려 기도 를 막는 수가 있다. 어린이 머리쪽에 앉 아 양손으로 턱아래를 밀어내는 방식이 좋다.',
                            #2 수족마비
                            '우황청심환이나 물을 함부로 먹이지 않는다.마비를 풀어주거나 의식을 유지시킬 목적으로 환자를 흔들거나 때리는 행위를 하지 않는다.사고나 외상으로 인한 마비증세의 경우, 절대로 환자를 움직이게 하지 않는다.',
                            #3 뇌경색
                             '혈압을 높이는 손을 따는 행동등을 하면안된다.폐렴을 유발할 수 있기 때문에 환자에게 먹을 것이나 마실 것을 주지 않는다.',
                            #4 심근경색
                            '이송할 때는 환자가 움직이거나 여러 환경적 요인에 의하여 스트레스를 받게 되면 심근경색을 악화시키게 되므로 조용하게 하며, 움직이지 않도록 한다.',
                            #5 복부창상
                             '물 뿐만 아니라 어떤 음식물도 제공해서는 안 된다.환자가 손상된 부분을 만지지 못하게 한다.',
                            #6 손가락절단
                             '절단 부위에 지혈제를 뿌리지 않는다.물 또는 얼음에 절단된 신체부위를 직접 담그지 않는다.과도하게 오랫동안 지혈대를 적용하지 않는다.',
                            #7 개에게물림 ㅇ
                             '소주 또는 된장 등의 민간요법을 피한다.개의 이상행동 등을 관찰해야 하므로 개를 죽이거나 다른 곳으로 보내는 행위는 피한다.',
                            #8 신체절단
                            '물이 새어 들어가면 조직이 물에 불어서 재 접합 할 수 없어진다. 얼음이 너무 많거나 물이 없으면 조직이 직접 얼음에 닿으면서 동결되게 되어 재 접합이 어려워진다.',
                            #9 위경련
                            '위경련으로 인한 어지럼증이 발생하거나 탈수 증상으로 기절하면 체액이 손실되어 신체가 쇼크 상황으로 들어가 심장,간,뇌,신장 손상이 발생할 수 있으므로 주의해야한다.',
                            #10 혈액투석 MKioskTy7
                            '응급실로 내원하세요',
                            #11 신장투석 MKioskTy7
                            '응급실로 내원하세요',
                            #12 조산 MKioskTy8
                            '응급실로 내원하세요',
                            #13 과호흡
                            '비닐봉지는 질식의 위험이 있기 때문에 공기가 잘 통하는 종이봉투를 사용한다.',
                            #14 소아머리손상
                             '의식이 혼미한 경우 찬물이나 기응환 등을 먹이지 않는다.꼭 이동해야 하는 상황이 아니라면 119에 신고하고 아이를 그대로 눕혀둔다.',
                            #15 소아열성경련
                             '혀가 물리지 않는 상태라면 입을 꽉 물고 있을 때 강제로 입을 벌리려 하면 안된다.아기에게는 최대한 안정을 취해주어야 하므로 아기를 안고 흔드는 것은 좋지 않다.',
                            #16 소아구토 MKioskTy10
                            '구토를 하게 되면 보통 위가 쉬도록 3~4시간 정도 아무것도 먹이지 않는다. 구토가 진정되면 물 종류부터 소량씩 시작해야한다. 머리를 다친 후 하는 구토 시에는 바로 응급실로 가야한다.',
                            #17 화상
                             '물집은 터트리면 안된다.간장, 된장 등을 바르는 민간요법은 감염의 위험이 있기 때문에 쓰지 않아야 한다.',
                             ],         
                             })

# department table 과 symptom table 1 : n
deptId=[]
for i in symptom.deptEng:
    deptId.append(department[department['deptEng']==i].index[0]+1)
symptom['DepartmentId']=deptId

hospital.index=hospital.index + 1

hospital.to_sql(name='hospital', con=connection, if_exists='append', index=False)
department.to_sql(name='department', con=connection, if_exists='append', index=False)
intermediate.to_sql(name='intermediate', con=connection, if_exists='append', index=False)
symptom.to_sql(name='symptom', con=connection, if_exists='append', index=False)

metadata = db.MetaData()
host = db.Table('hospital', metadata, autoload=True, autoload_with=engine)
query_select=db.select(host)
end=len(engine.execute(query_select).fetchall())

for i in range(1,end+1):
    engine.execute(db.update(host).where(host.c.id == i).values(image="https://hack4.s3.ap-northeast-2.amazonaws.com/hostImage/"+str(i)+".jpg"))

symp = db.Table('symptom', metadata, autoload=True, autoload_with=engine)
symt_query_select=db.select(host)
symp_end=len(engine.execute(query_select).fetchall())

for i in range(1,symp_end+1):
    engine.execute(db.update(symp).where(symp.c.id == i).values(firstImage="https://hack4.s3.ap-northeast-2.amazonaws.com/symptom/"+str(i)+".jpg"))
    engine.execute(db.update(symp).where(symp.c.id == i).values(noticeImage="https://hack4.s3.ap-northeast-2.amazonaws.com/notice/1.jpg"))
