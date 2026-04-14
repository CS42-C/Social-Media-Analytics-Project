import sqlite3
import pandas as pd
from sklearn.preprocessing import LabelEncoder
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split

DB = r'C:\Users\Francisco Costa\Desktop\Projects\Social Media Dashboard\files\social_media_analytics.db'

conn = sqlite3.connect(DB)
customers = pd.read_sql('SELECT * FROM Customers', conn)
orders    = pd.read_sql('SELECT * FROM Orders', conn)
conn.close()

orders['OrderDate'] = pd.to_datetime(orders['OrderDate'])
REF = pd.Timestamp('2024-12-31')

cust_agg = orders.groupby('CustomerID').agg(
    TotalOrders=('OrderID','count'),
    TotalRevenue=('Revenue','sum'),
    AvgOrderValue=('Revenue','mean'),
    LastOrderDate=('OrderDate','max'),
    FirstOrderDate=('OrderDate','min'),
    TotalReturns=('IsReturn','sum')
).reset_index()

cust_agg['DaysSinceLastOrder'] = (REF - cust_agg['LastOrderDate']).dt.days
cust_agg['CustomerAge']        = (REF - cust_agg['FirstOrderDate']).dt.days
cust_agg['IsChurned']          = (cust_agg['DaysSinceLastOrder'] > 90).astype(int)

model_df = customers.merge(cust_agg, on='CustomerID', how='inner')

le = LabelEncoder()
model_df['AcqChannel_enc'] = le.fit_transform(model_df['AcqChannel'])
model_df['Segment_enc']    = le.fit_transform(model_df['Segment'])
model_df['Gender_enc']     = le.fit_transform(model_df['Gender'])

FEATURES = ['DaysSinceLastOrder','TotalOrders','TotalRevenue',
            'AvgOrderValue','CustomerAge','Age',
            'AcqChannel_enc','Segment_enc','TotalReturns']

X = model_df[FEATURES].fillna(0)
y = model_df['IsChurned']

X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.2, random_state=42, stratify=y
)

rf = RandomForestClassifier(n_estimators=50, max_depth=4, random_state=42, class_weight='balanced')
rf.fit(X_train, y_train)
print('Model trained')

all_prob = rf.predict_proba(X.fillna(0))[:,1]
all_pred = rf.predict(X.fillna(0))

model_df['ChurnProbability'] = all_prob.round(4)
model_df['ChurnPrediction']  = all_pred
model_df['ChurnRisk'] = pd.cut(
    model_df['ChurnProbability'],
    bins=[0, 0.3, 0.6, 1.0],
    labels=['Low Risk','Medium Risk','High Risk']
).astype(str)

churn_export = model_df[[
    'CustomerID','ChurnProbability','ChurnPrediction',
    'ChurnRisk','DaysSinceLastOrder','TotalOrders',
    'TotalRevenue','AvgOrderValue'
]].copy()

conn = sqlite3.connect(DB)
churn_export.to_sql('Churn_Predictions', conn, if_exists='replace', index=False)
conn.close()

print(f'Done - {len(churn_export)} customers exported')
print(churn_export['ChurnRisk'].value_counts().to_string())