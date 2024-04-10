# -*- coding:UTF-8 -*-

import arch
import warnings
import numpy as np
import matplotlib.pyplot as plt
from tqdm import tqdm
from scipy.special import gamma
from sklearn.metrics import mean_squared_error
from statsmodels.tsa.stattools import adfuller
warnings.simplefilter('ignore')


class EstimateSigma:
    """使用ewma, arch,和garch模型估计sigma"""

    def __init__(self, asset, df, window, lamb, p, q, return_type):
        '''
        :param asset: 标的代码
        :param df: dataframe, 列包括：交易日，开高低收价格，收益率，对数收益率。
        :param window: 测试窗口长度
        :param lamb: lambda, 用于ewma公式
        :param p: arch效应的滞后阶数
        :param q: garch效应的滞后阶数
        :param return_type:
        :param root: 文件保存路径
        '''
        self.asset = asset
        self.df = df
        self.window = window
        self.lamb = lamb
        self.p = p
        self.q = q
        self.return_type = return_type

    def yang_zhang_sigma(self, alpha = 0.34):
        '''
        计算yang_zhang_sigma
        :param alpha:
        '''
        data = self.df.sort_values(['symbol', 'date']).copy()

        data['c2c'] = data['close'].div(data['pre_close'])
        data['ln_c2c'] = np.log(data['c2c'])
        data['c2c_sigma'] = data.groupby('symbol')['ln_c2c'].rolling(self.window, min_periods=1).std(ddof=1).values

        data['o'] = data['open'].div(data['pre_close'])
        data['ln_o'] = np.log(data['o'])
        data['o_sigma'] = data.groupby('symbol')['ln_o'].rolling(self.window, min_periods=1).std(ddof=1).values

        data['c'] = data['close'].div(data['open'])
        data['ln_c'] = np.log(data['c'])
        data['c_sigma'] = data.groupby('symbol')['ln_c'].rolling(self.window, min_periods=1).std(ddof=1).values

        data['h_c'] = np.log(data['high'].div(data['close']))
        data['h_o'] = np.log(data['high'].div(data['open']))
        data['l_c'] = np.log(data['low'].div(data['close']))
        data['l_o'] = np.log(data['low'].div(data['open']))

        data['rsy'] = data['h_c'] * data['h_o'] + data['l_c'] * data['l_o']
        data['rsy_sigma'] = data.groupby('symbol')['rsy'].rolling(self.window).mean().values
        data['rsy_sigma'] = np.sqrt(data['rsy_sigma'])

        # weighted average
        k = alpha / ((1 + alpha) + (self.window + 1) / (self.window - 1))

        data['yang_zhang_sigma'] = np.sqrt(
            pow(data['o_sigma'], 2) + k * pow(data['c_sigma'], 2) + (1 - k) * pow(data['rsy_sigma'], 2))

        self.df = self.df.merge(data[['date','yang_zhang_sigma']], on=['date'], how='inner')


    def adf_test(self):
        adf_test_result = adfuller(self.df[self.return_type])
        return adf_test_result[1]  # 输出P值


    def estimate(self):
        '''估计波动率'''
        total_iterations = total_iterations = len(self.df) - 2 * self.window
        with tqdm(total=total_iterations) as pbar:
            for i in range(self.window, len(self.df)-self.window):
                arch_model = arch.arch_model(self.df[self.return_type].iloc[i-self.window:i-1], vol='GARCH', p=self.p, q=0, o=0, dist='normal')
                garch_model = arch.arch_model(self.df[self.return_type].iloc[i-self.window:i-1], vol='GARCH', p=self.p, q=self.q, o=0, dist='normal')
                arch_model_fit = arch_model.fit(disp='off')
                garch_model_fit = garch_model.fit(disp='off')
                arch_forecast = arch_model_fit.forecast(horizon=1)
                garch_forecast = garch_model_fit.forecast(horizon=1)
                self.df.loc[i, 'ewma_sigma'] = np.sqrt(
                    self.lamb * self.df.loc[i-1, 'yang_zhang_sigma'] ** 2 + (1 - self.lamb) * self.df.loc[i, 'return'] ** 2)
                self.df.loc[i, 'arch_sigma'] = np.sqrt(arch_forecast.variance.values[-1,0])
                self.df.loc[i, 'arch_AIC'] = arch_model_fit.aic
                self.df.loc[i, 'arch_BIC'] = arch_model_fit.bic
                self.df.loc[i, 'garch_sigma'] = np.sqrt(garch_forecast.variance.values[-1,0])
                self.df.loc[i, 'garch_AIC'] = garch_model_fit.aic
                self.df.loc[i, 'garch_BIC'] = garch_model_fit.bic
                self.df.loc[i, f'fut_yang_zhang_sigma']= self.df.loc[i+1, 'yang_zhang_sigma']
                pbar.set_description(f"Processing item {i - self.window + 1}/{total_iterations}")
                pbar.update(1)
            print("Loop completed.")
        # self.df.to_parquet()


    def b(self):
        '''标准差修正'''
        return np.sqrt(2 / self.window) * gamma(self.window / 2) / gamma((self.window - 1) / 2)

    def corrected_standard_deviation(self, variance_estimate):
        '''
        计算修正后的标准差估计
        :param variance_estimate: 需要修正的波动率估计量
        '''
        self.df[variance_estimate] = np.sqrt(self.df[variance_estimate]) / self.b()

    def plot_trend(self):
        '''可视化波动率趋势'''
        temp_df = self.df.dropna()

        plt.subplots(4,1, figsize=(30,45))
        plt.rcParams['font.size'] = 22
        plt.rcParams["figure.autolayout"] = True

        x = temp_df['date']
        plt.subplot(411)
        plt.plot(x, temp_df[f'fut_yang_zhang_sigma'], label='future_yang_zhang_sigma', alpha=0.7)
        plt.plot(x, temp_df['yang_zhang_sigma'], label=f'yang_zhang_sigma', alpha=0.7)
        plt.xticks(rotation=30, fontsize=16)
        plt.yticks(fontsize=20)
        plt.gca().xaxis.set_major_locator(plt.MaxNLocator(10))
        plt.title('History Estimate')
        plt.legend()
        plt.grid()

        plt.subplot(412)
        plt.plot(x, temp_df[f'fut_yang_zhang_sigma'], label='future_yang_zhang_sigma', alpha=0.7)
        plt.plot(x, temp_df['ewma_sigma'], label='EWMA', alpha=0.7)
        plt.xticks(rotation=30, fontsize=16)
        plt.yticks(fontsize=20)
        plt.gca().xaxis.set_major_locator(plt.MaxNLocator(10))
        plt.title('EWMA Estimate')
        plt.legend()
        plt.grid()

        plt.subplot(413)
        plt.plot(x, temp_df[f'fut_yang_zhang_sigma'], label='future_yang_zhang_sigma', alpha=0.7)
        plt.plot(x, temp_df['arch_sigma'], label=f'ARCH({self.p})', alpha=0.7)
        plt.xticks(rotation=30, fontsize=16)
        plt.yticks(fontsize=20)
        plt.gca().xaxis.set_major_locator(plt.MaxNLocator(10))
        plt.title('ARCH Estimate')
        plt.legend()
        plt.grid()

        plt.subplot(414)
        # plt.plot(x, temp_df['return'], label='Return', alpha=0.7)
        plt.plot(x, temp_df[f'fut_yang_zhang_sigma'], label='future_yang_zhang_sigma', alpha=0.7)
        plt.plot(x, temp_df['garch_sigma'], label=f'GARCH({self.p},{self.q})', alpha=0.7)
        plt.xticks(rotation=30, fontsize=16)
        plt.yticks(fontsize=20)
        plt.gca().xaxis.set_major_locator(plt.MaxNLocator(10))
        plt.title('GARCH Estimate')
        plt.legend()
        plt.grid()

        # plt.tight_layout()
        plt.suptitle(f'{self.asset}_window={self.window}_lambda={self.lamb}_p={self.p}_q={self.q}_return={self.return_type}')
        plt.show()

    def run(self):
        self.yang_zhang_sigma()

        if self.adf_test() < 0.05:
            print(f'{self.return_type}序列平稳，通过ADF检验\n')
            self.estimate()
        else:
            print(f'{self.return_type}序列不平稳\n')

        self.df.dropna(inplace=True)

        sigma_list = ['yang_zhang_sigma', 'ewma_sigma', 'arch_sigma', 'garch_sigma']
        for sigma in sigma_list + [f'fut_yang_zhang_sigma']:
            self.corrected_standard_deviation(sigma)

        self.result_dict = {}
        for sigma in sigma_list:
            mse = mean_squared_error(self.df[f'fut_yang_zhang_sigma'], self.df[sigma])
            self.result_dict[f'{sigma}_mse'] = np.round(mse, 8)
            print(f'mse of {sigma}:', mse)

        # print('mean ARCH AIC = ', self.df['arch_AIC'].mean())
        # print('mean GARCH AIC = ', self.df['garch_AIC'].mean())
        # print('mean ARCH BIC = ', self.df['arch_BIC'].mean())
        # print('mean GARCH BIC = ', self.df['garch_BIC'].mean())

        self.plot_trend()
