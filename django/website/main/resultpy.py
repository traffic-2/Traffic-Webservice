# from statsmodels.tsa.arima_model import ARIMAResults
import pickle
import pandas as pd
from fbprophet import Prophet
import matplotlib.pyplot as plt


def model_load():
    # 모델 로드
    loaded_model = pickle.load(open('main/model3.pkl', 'rb'))

    # forecast_data = loaded_model.predict(periods=1, freq='d')

    future_data = loaded_model.make_future_dataframe(periods=24, freq='d')
    forecast_data = loaded_model.predict(future_data)

    pred_y = forecast_data.yhat.values[60*24: 61*24]

    return pred_y
