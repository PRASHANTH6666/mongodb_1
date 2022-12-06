from pymongo import MongoClient
client=MongoClient()
print(client.list_database_names())
db=client.students
print(db.list_collection_names())
np=db.details.find()
for i in np:
    print(i)
#1.student scored highest mark
pn=db.details.aggregate([{'$unwind':'$scores'},{'$group':{'_id':'$name','total_score':{'$sum':'$scores.score'}}},{'$sort':{'total_score':-1}},{'$limit':1},{'$out':'first_mark'}])
for i in pn:
    print(i)

#2.student scoes below 40

a={'$unwind':'$scores'}
b={'$match':{'$and':[{'scores.type':'exam'},{'scores.score':{'$lt':40}}]}}
c={'$out':'below_40'}

m=db.details.aggregate([a,b,c])
for i in m:
    print(i)

#3.total and average score of all types(exam, home ,quiz)

n=db.details.aggregate([{'$unwind':'$scores'},{'$group':{'_id':'$name','total_score':{'$sum':'$scores.score'},'avg_score':{'$avg':'$scores.score'}}},{'$out':'total and average'}])

for i in n:
    print(i)

#4.avergae score
w=db.detils.aggregate([{'$unwind': '$scores'} ,{'$group': {'_id' :{'type' : "$scores.type" },  'Average_score':{'$avg':"$scores.score"}}}])
for i in w:
  print(i)
#total score

q=db.etails.aggregate([{'$unwind': '$scores'} ,{'$group': {'_id' :{'type' : "$scores.type" },  'Total':{'$sum':"$scores.score"}}}])
for i in q:
  print(i)

#5.below avg.
querry={"scores":{"score":{'$lte':48.0},'type':'exam'}}
bel_avg=db.details.find(querry,{'_id':1,'name':1,'scores':1})
print(bel_avg)
np.insert_one(bel_avg)
#above avg.

querry1={"scores":{"score":{'$gte':48.0},'type':'exam'}}
abv_avg=db.details.find(querry1,{'_id':1,'name':1,'scores':1})
print(abv_avg)
np.insert_one(abv_avg)

#6 failed students

records=np.failed_students
failed_students=records.find({'status':'fail'},{'_id':1,'name':1,'scores':1})
records.insert_one(failed_students)

#7 pass students

records_1=np.passed_students
passed_students=records_1.find({'status':'pass'},{'_id':1,'name':1,'scores':1})
records_1.insert_one(passed_students)