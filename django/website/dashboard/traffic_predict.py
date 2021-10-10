# from statsmodels.tsa.arima_model import ARIMAResults
import pickle
import pandas as pd
from fbprophet import Prophet
import matplotlib.pyplot as plt


def model_load():
    # 모델 로드
    loaded_model1 = pickle.load(open('dashboard/df1.pkl', 'rb'))
    loaded_model2 = pickle.load(open('dashboard/df2.pkl', 'rb'))
    loaded_model3 = pickle.load(open('dashboard/df3.pkl', 'rb'))
    loaded_model4 = pickle.load(open('dashboard/df4.pkl', 'rb'))
    loaded_model5 = pickle.load(open('dashboard/df5.pkl', 'rb'))

    future_data1 = loaded_model1.make_future_dataframe(periods=1*24, freq='H')
    forecast_data1 = loaded_model1.predict(future_data1)

    future_data2 = loaded_model2.make_future_dataframe(periods=1*24, freq='H')
    forecast_data2 = loaded_model2.predict(future_data2)

    future_data3 = loaded_model3.make_future_dataframe(periods=1*24, freq='H')
    forecast_data3 = loaded_model3.predict(future_data3)

    future_data4 = loaded_model4.make_future_dataframe(periods=1*24, freq='H')
    forecast_data4 = loaded_model4.predict(future_data4)

    future_data5 = loaded_model5.make_future_dataframe(periods=1*24, freq='H')
    forecast_data5 = loaded_model5.predict(future_data5)

    pred_y1 = forecast_data1.yhat.tail(24*1)
    pred_y2 = forecast_data2.yhat.tail(24*1)
    pred_y3 = forecast_data3.yhat.tail(24*1)
    pred_y4 = forecast_data4.yhat.tail(24*1)
    pred_y5 = forecast_data5.yhat.tail(24*1)

    return pred_y1, pred_y2, pred_y3, pred_y4, pred_y5

#model_load()